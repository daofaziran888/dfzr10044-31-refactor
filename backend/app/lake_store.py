"""湖表存储引擎：文件注册表、分区清单、快照链、水位存储与批次数据。

本模块提供跨服务共享的不可变数据层：
- 文件注册：每个分区文件有唯一 file_id、所属分区、所属湖表、行数、字节数
- 快照链：每个湖表按时间线记录快照，含 file_ids、manifest、父快照指针
- 水位：每张湖表的已提交/待提交水位记录
- 批次数据：入湖批次的行级数据存储，含校验值
- 隔离区：被质量规则拦截的数据行，按批次隔离
- 写锁：并发提交冲突检测

设计原则：
- 所有写操作在提交前必须拿到全局锁
- 快照只在事务完整落湖后标记为可用
- 归并时先写入新文件清单，再原子地替换旧文件列表
"""
from __future__ import annotations

import copy
import hashlib
import threading
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any, Iterator


# ----------------------------- 文件注册 -----------------------------

@dataclass
class PartitionFile:
    """分区文件：每个文件属于一个分区。

    status=active 的文件才属于当前正式清单；staged 是已经写好但尚未发布的
    临时结果，replaced/abandoned 只保留在登记表中用于核对和审计。
    """

    file_id: str
    lake_table: str
    partition_key: str
    file_rows: int
    file_bytes: int
    file_manifest: str  # 文件内容摘要（MD5）
    created_at: float
    status: str = "active"
    source_file_ids: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


# ----------------------------- 快照 -----------------------------

@dataclass
class LakeSnapshot:
    """湖表快照：记录某个时刻整个湖表的分区文件清单。"""
    snapshot_id: str
    lake_table: str
    parent_snapshot: str | None
    file_ids: list[str]
    added_rows: int
    deleted_rows: int
    manifest_checksum: str
    status: str  # generating | available | expired | rolled_back
    created_at: float


# ----------------------------- 水位 -----------------------------

@dataclass
class WatermarkRecord:
    """入湖水位追踪：记录已提交和待提交水位。"""
    lake_table: str
    field_name: str
    committed_value: str
    pending_value: str | None
    strategy: str  # append | merge | overwrite
    last_advanced_at: float


# ----------------------------- 批次数据 -----------------------------

@dataclass
class BatchRecord:
    """入湖批次：一次入湖操作导出的一组数据行。"""
    batch_id: str
    lake_table: str
    source_system: str
    rows: list[dict[str, Any]]  # 实际数据行
    row_count: int
    window_start: str
    window_end: str
    checksum: str
    snapshot_id: str | None  # 落湖后分配的 snap
    status: str  # pending | ingesting | ingested | failed
    created_at: float


# ----------------------------- 写锁 -----------------------------

class WriteLock:
    """全局写入锁：同一湖表同时只能有一个写入者提交。"""

    def __init__(self) -> None:
        self._locks: dict[str, threading.Lock] = {}
        self._lock = threading.Lock()

    def acquire(self, lake_table: str) -> threading.Lock:
        with self._lock:
            if lake_table not in self._locks:
                self._locks[lake_table] = threading.Lock()
            table_lock = self._locks[lake_table]
        table_lock.acquire()
        return table_lock

    def release(self, lake_table: str) -> None:
        with self._lock:
            lock = self._locks.get(lake_table)
        if lock is not None:
            try:
                lock.release()
            except RuntimeError:
                pass


# ----------------------------- 清单代次冲突 -----------------------------

class ManifestGenerationConflict(RuntimeError):
    """发布时期望代次与分区当前代次不一致。"""

    def __init__(
        self,
        message: str,
        *,
        lake_table: str | None = None,
        partition_key: str | None = None,
        expected_generation: int | None = None,
        current_generation: int | None = None,
        current_file_ids: list[str] | None = None,
        overlap: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.lake_table = lake_table
        self.partition_key = partition_key
        self.expected_generation = expected_generation
        self.current_generation = current_generation
        self.current_file_ids = list(current_file_ids or [])
        self.overlap = dict(overlap or {})

    def to_dict(self) -> dict[str, Any]:
        return {
            "error": "manifest_generation_conflict",
            "message": str(self),
            "lake_table": self.lake_table,
            "partition_key": self.partition_key,
            "expected_generation": self.expected_generation,
            "expected_version": self.expected_generation,
            "current_generation": self.current_generation,
            "current_version": self.current_generation,
            "current_file_ids": list(self.current_file_ids),
            "overlap": copy.deepcopy(self.overlap),
            "conflict": True,
        }


# 兼容不同调用方可能使用的异常命名。
ManifestConflict = ManifestGenerationConflict
ManifestConflictError = ManifestGenerationConflict


# ----------------------------- 存储引擎 -----------------------------

class LakeStore:
    """湖表存储引擎主类。

    管理三大维度：
    - files: 分区文件注册表
    - snapshots: 快照链
    - watermarks: 水位
    - batches: 批次数据
    - quarantine: 隔离区数据
    - write_lock: 并发提交的控制锁
    """

    def __init__(self) -> None:
        # 文件注册表：lake_table -> {file_id -> PartitionFile}
        self._files: dict[str, dict[str, PartitionFile]] = {}

        # 快照链：lake_table -> list[LakeSnapshot]（时间序）
        self._snapshots: dict[str, list[LakeSnapshot]] = {}

        # 水位：lake_table -> WatermarkRecord
        self._watermarks: dict[str, WatermarkRecord] = {}

        # 批次数据：batch_id -> BatchRecord
        self._batches: dict[str, BatchRecord] = {}

        # 隔离区：batch_id -> list[dict]（被拦截的数据行）
        self._quarantine: dict[str, list[dict[str, Any]]] = {}

        # 写锁
        self._write_lock = WriteLock()

        # 归并中断恢复检查点：merge_id -> {partial_file_ids: [...], ...}
        self._merge_checkpoints: dict[str, dict[str, Any]] = {}

        # 分区正式清单及其乐观代次。键为 (lake_table, partition_key)。
        self._manifest_lock = threading.RLock()
        self._partition_manifests: dict[tuple[str, str], list[str]] = {}
        self._partition_generations: dict[tuple[str, str], int] = {}

        # 已确认发布记录和按原申请编号保存的处置结果。
        self._publications: dict[str, dict[str, Any]] = {}
        self._publication_order: list[str] = []
        self._idempotency_index: dict[tuple[str, str], str] = {}
        self._operation_results: dict[tuple[str, str], dict[str, Any]] = {}
        self._merge_results: dict[str, dict[str, Any]] = {}
        self._compact_results: dict[str, dict[str, Any]] = {}

    def reset(self) -> None:
        """清空内存湖仓状态，主要供隔离测试使用。"""
        self.__init__()

    def clear(self) -> None:
        self.reset()

    @staticmethod
    def _manifest_key(lake_table: str, partition_key: str | None) -> tuple[str, str]:
        return lake_table, "" if partition_key is None else str(partition_key)

    @contextmanager
    def manifest_transaction(self) -> Iterator[None]:
        """串行化同一进程内的清单校核与替换。"""
        with self._manifest_lock:
            yield

    # ---- 分区清单与代次 ----

    def get_partition_generation(self, lake_table: str, partition_key: str) -> int:
        with self._manifest_lock:
            return self._partition_generations.get(
                self._manifest_key(lake_table, partition_key), 0
            )

    # 常见同义命名，便于不同调用习惯。
    def get_generation(self, lake_table: str, partition_key: str) -> int:
        return self.get_partition_generation(lake_table, partition_key)

    def current_generation(self, lake_table: str, partition_key: str) -> int:
        return self.get_partition_generation(lake_table, partition_key)

    def is_generation_current(
        self, lake_table: str, partition_key: str, expected_generation: int
    ) -> bool:
        return self.get_partition_generation(lake_table, partition_key) == expected_generation

    def is_manifest_current(
        self, lake_table: str, partition_key: str, expected_generation: int
    ) -> bool:
        return self.is_generation_current(lake_table, partition_key, expected_generation)

    def get_partition_version(self, lake_table: str, partition_key: str) -> int:
        return self.get_partition_generation(lake_table, partition_key)

    def current_version(self, lake_table: str, partition_key: str) -> int:
        return self.get_partition_generation(lake_table, partition_key)

    def is_version_current(
        self, lake_table: str, partition_key: str, expected_version: int
    ) -> bool:
        return self.is_generation_current(lake_table, partition_key, expected_version)

    def get_partition_manifest(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        """返回分区当前正式清单快照；staged/replaced 文件不在 file_ids 中。"""
        key = self._manifest_key(lake_table, partition_key)
        with self._manifest_lock:
            table_files = self._files.get(lake_table, {})
            stored_ids = self._partition_manifests.get(key)
            if stored_ids is None:
                stored_ids = [
                    fid for fid, pf in table_files.items()
                    if pf.partition_key == partition_key and pf.status == "active"
                ]

            active_ids: list[str] = []
            seen: set[str] = set()
            for fid in stored_ids:
                pf = table_files.get(fid)
                if (
                    pf is not None
                    and pf.partition_key == partition_key
                    and pf.status == "active"
                    and fid not in seen
                ):
                    active_ids.append(fid)
                    seen.add(fid)

            # 兼容直接操作 _files 或历史对象：补登尚未进入清单的 active 文件。
            for fid, pf in table_files.items():
                if (
                    pf.partition_key == partition_key
                    and pf.status == "active"
                    and fid not in seen
                ):
                    active_ids.append(fid)
                    seen.add(fid)

            self._partition_manifests[key] = active_ids
            checksum = hashlib.md5(",".join(sorted(active_ids)).encode()).hexdigest()
            files = [table_files[fid] for fid in active_ids if fid in table_files]
            return {
                "lake_table": lake_table,
                "partition_key": partition_key,
                "generation": self._partition_generations.get(key, 0),
                "version": self._partition_generations.get(key, 0),
                "manifest_version": self._partition_generations.get(key, 0),
                "file_ids": list(active_ids),
                "files": files,
                "manifest_checksum": checksum,
                "checksum": checksum,
                "file_count": len(active_ids),
                "total_rows": sum(pf.file_rows for pf in files),
                "total_bytes": sum(pf.file_bytes for pf in files),
            }

    def get_manifest_snapshot(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        return self.get_partition_manifest(lake_table, partition_key)

    read_manifest = get_manifest_snapshot
    get_file_manifest = get_manifest_snapshot

    def list_manifest_files(self, lake_table: str, partition_key: str) -> list[PartitionFile]:
        return list(self.get_partition_manifest(lake_table, partition_key)["files"])

    def list_staged_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        files = self.list_files(lake_table, partition_key, include_staged=True)
        return [pf for pf in files if pf.status == "staged"]

    def list_temporary_files(self, lake_table: str, partition_key: str | None = None) -> list[PartitionFile]:
        return self.list_staged_files(lake_table, partition_key)

    def _bump_generation(self, key: tuple[str, str]) -> int:
        generation = self._partition_generations.get(key, 0) + 1
        self._partition_generations[key] = generation
        return generation

    # ---- 文件操作 ----

    def register_file(
        self,
        lake_table: str,
        partition_key: str,
        rows: int,
        bytes_: int,
        manifest: str | None = None,
        active: bool = True,
        staged: bool = False,
        source_file_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
        **kwargs: Any,
    ) -> PartitionFile:
        temporary = bool(kwargs.get("temporary", False))
        status = kwargs.get("status")
        if status is None:
            status = "staged" if staged or temporary or not active else "active"
        file_id = f"{lake_table}-p{partition_key}-{uuid.uuid4().hex[:8]}"
        checksum = manifest or hashlib.md5(f"{lake_table}:{partition_key}:{time.time()}".encode()).hexdigest()
        pf = PartitionFile(
            file_id=file_id,
            lake_table=lake_table,
            partition_key=partition_key,
            file_rows=rows,
            file_bytes=bytes_,
            file_manifest=checksum,
            created_at=time.time(),
            status=str(status),
            source_file_ids=list(source_file_ids or []),
            metadata=dict(metadata or {}),
        )
        with self._manifest_lock:
            self._files.setdefault(lake_table, {})[file_id] = pf
            if pf.status == "active":
                key = self._manifest_key(lake_table, partition_key)
                ids = self._partition_manifests.setdefault(key, [])
                if file_id not in ids:
                    ids.append(file_id)
                self._bump_generation(key)
        return pf

    def stage_file(
        self,
        lake_table: str,
        partition_key: str,
        rows: int,
        bytes_: int,
        manifest: str | None = None,
        source_file_ids: list[str] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> PartitionFile:
        """登记已写好但尚未进入正式清单的临时结果。"""
        return self.register_file(
            lake_table,
            partition_key,
            rows,
            bytes_,
            manifest,
            active=False,
            staged=True,
            source_file_ids=source_file_ids,
            metadata=metadata,
        )

    def list_files(
        self,
        lake_table: str,
        partition_key: str | None = None,
        *,
        include_staged: bool = False,
        include_replaced: bool = False,
        include_inactive: bool = False,
    ) -> list[PartitionFile]:
        with self._manifest_lock:
            files = list(self._files.get(lake_table, {}).values())
            if partition_key is not None:
                files = [f for f in files if f.partition_key == partition_key]

            if include_inactive or (include_staged and include_replaced):
                return files
            if include_staged:
                return [f for f in files if f.status in ("active", "staged")]
            if include_replaced:
                return [f for f in files if f.status in ("active", "replaced")]
            return [f for f in files if f.status == "active"]

    def get_file(
        self, lake_table: str, file_id: str, *, active_only: bool = False
    ) -> PartitionFile | None:
        pf = self._files.get(lake_table, {}).get(file_id)
        if pf is None:
            return None
        if active_only and pf.status != "active":
            return None
        return pf

    def remove_files(self, lake_table: str, file_ids: list[str]) -> int:
        removed = 0
        with self._manifest_lock:
            table_files = self._files.get(lake_table, {})
            affected: set[tuple[str, str]] = set()
            for fid in file_ids:
                pf = table_files.get(fid)
                if pf is not None:
                    affected.add(self._manifest_key(pf.lake_table, pf.partition_key))
                    del table_files[fid]
                    removed += 1

            for key in affected:
                ids = self._partition_manifests.get(key)
                if ids is not None:
                    removed_set = set(file_ids)
                    self._partition_manifests[key] = [fid for fid in ids if fid not in removed_set]
                self._bump_generation(key)
        return removed

    def replace_file_list(
        self,
        lake_table: str,
        old_file_ids: list[str],
        new_files: list[PartitionFile],
        *,
        expected_generation: int | None = None,
        expected_version: int | None = None,
        partition_key: str | None = None,
        publication_id: str | None = None,
    ) -> dict[str, Any] | None:
        """原子替换正式清单。

        旧 ID 从登记表删除，新文件置为 active。调用方可传入
        expected_generation 做乐观代次校核；共用发布器通常先做更完整的
        幂等和重叠核对。
        """
        old_ids = list(old_file_ids)
        outputs = list(new_files)
        if expected_generation is None:
            expected_generation = expected_version
        with self._manifest_lock:
            table_files = self._files.setdefault(lake_table, {})
            affected_partitions = {pf.partition_key for pf in outputs}
            for fid in old_ids:
                old_pf = table_files.get(fid)
                if old_pf is not None:
                    affected_partitions.add(old_pf.partition_key)
            if partition_key is not None:
                affected_partitions.add(partition_key)

            if expected_generation is not None:
                for part in affected_partitions or ({partition_key} if partition_key else set()):
                    if part is None:
                        continue
                    key = self._manifest_key(lake_table, part)
                    current = self._partition_generations.get(key, 0)
                    if current != expected_generation:
                        snap = self.get_partition_manifest(lake_table, part)
                        raise ManifestGenerationConflict(
                            "分区清单代次已变化，拒绝覆盖发布",
                            lake_table=lake_table,
                            partition_key=part,
                            expected_generation=expected_generation,
                            current_generation=current,
                            current_file_ids=snap["file_ids"],
                        )

            for nf in outputs:
                nf.status = "active"
                if publication_id is not None:
                    nf.metadata["publication_id"] = publication_id
                table_files[nf.file_id] = nf

            old_id_set = set(old_ids)
            for fid in old_ids:
                old_pf = table_files.get(fid)
                if old_pf is not None:
                    # 保留对象上的来源信息后删除登记，维持原有“旧文件消失”语义。
                    old_pf.status = "replaced"
                table_files.pop(fid, None)

            generations: dict[str, int] = {}
            for part in affected_partitions:
                key = self._manifest_key(lake_table, part)
                ids = self._partition_manifests.setdefault(key, [])
                ids = [fid for fid in ids if fid not in old_id_set]
                existing = set(ids)
                for nf in outputs:
                    if nf.partition_key == part and nf.file_id not in existing:
                        ids.append(nf.file_id)
                        existing.add(nf.file_id)
                self._partition_manifests[key] = ids
                generations[part] = self._bump_generation(key)

        if publication_id is not None or expected_generation is not None:
            return {
                "publication_id": publication_id,
                "lake_table": lake_table,
                "old_file_ids": old_ids,
                "new_file_ids": [pf.file_id for pf in outputs],
                "generations": generations,
            }
        return None

    # ---- 发布记录与处置结果 ----

    def record_publication(self, publication: dict[str, Any]) -> dict[str, Any]:
        with self._manifest_lock:
            pub_id = publication["publication_id"]
            record = dict(publication)
            self._publications[pub_id] = record
            if pub_id not in self._publication_order:
                self._publication_order.append(pub_id)
            op_type = record.get("operation_type", "manifest")
            op_id = record.get("operation_id")
            if op_id is not None:
                op_key = (str(op_type), str(op_id))
                self._idempotency_index[op_key] = pub_id
                scoped_key = (
                    str(op_type),
                    str(record.get("lake_table", "")),
                    str(record.get("partition_key", "")),
                    str(op_id),
                )
                self._idempotency_index[scoped_key] = pub_id
            idem_key = record.get("idempotency_key")
            if idem_key is not None:
                self._idempotency_index[("idempotency", str(idem_key))] = pub_id
                self._idempotency_index[(
                    "idempotency",
                    str(record.get("lake_table", "")),
                    str(record.get("partition_key", "")),
                    str(idem_key),
                )] = pub_id
            return record

    def get_publication(self, publication_id: str) -> dict[str, Any] | None:
        return self._publications.get(publication_id)

    def find_publication(
        self,
        operation_type: str | None = None,
        operation_id: str | None = None,
        idempotency_key: str | None = None,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any] | None:
        pub_id = None
        if idempotency_key is not None and lake_table is not None and partition_key is not None:
            pub_id = self._idempotency_index.get((
                "idempotency", str(lake_table), str(partition_key), str(idempotency_key)
            ))
        if pub_id is None and idempotency_key is not None:
            pub_id = self._idempotency_index.get(("idempotency", str(idempotency_key)))
        if pub_id is None and operation_type is not None and operation_id is not None:
            if lake_table is not None and partition_key is not None:
                pub_id = self._idempotency_index.get((
                    str(operation_type), str(lake_table), str(partition_key), str(operation_id)
                ))
            if pub_id is None:
                pub_id = self._idempotency_index.get((str(operation_type), str(operation_id)))
        return self._publications.get(pub_id) if pub_id else None

    def list_publications(
        self, lake_table: str | None = None, partition_key: str | None = None
    ) -> list[dict[str, Any]]:
        pubs = [self._publications[pid] for pid in self._publication_order]
        if lake_table is not None:
            pubs = [p for p in pubs if p.get("lake_table") == lake_table]
        if partition_key is not None:
            pubs = [p for p in pubs if p.get("partition_key") == partition_key]
        return pubs

    def find_overlapping_publication(
        self,
        lake_table: str,
        partition_key: str,
        input_file_ids: list[str] | set[str],
    ) -> dict[str, Any] | None:
        wanted = set(input_file_ids)
        candidates: list[tuple[int, float, dict[str, Any]]] = []
        for pub in self.list_publications(lake_table, partition_key):
            pub_inputs = set(pub.get("input_file_ids", []))
            pub_outputs = set(pub.get("output_file_ids", []))
            overlap = len(wanted & pub_inputs)
            if overlap == 0:
                # 也允许通过输出来源链核对。
                for fid in wanted:
                    pf = self.get_file(lake_table, fid)
                    if pf is not None and set(pf.source_file_ids) & wanted:
                        overlap += 1
            if overlap:
                candidates.append((overlap, float(pub.get("committed_at", 0.0)), pub))
        if not candidates:
            return None
        return max(candidates, key=lambda item: (item[0], item[1]))[2]

    def save_operation_result(
        self,
        kind: str,
        operation_id: str,
        result: dict[str, Any],
        *,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any]:
        record = dict(result)
        record.setdefault("kind", kind)
        record.setdefault("operation_type", kind)
        record["operation_id"] = operation_id
        if lake_table is not None:
            record.setdefault("lake_table", lake_table)
        if partition_key is not None:
            record.setdefault("partition_key", partition_key)
        table_scope = record.get("lake_table") or ""
        partition_scope = record.get("partition_key") or ""
        with self._manifest_lock:
            self._operation_results[(kind, str(operation_id))] = record
            self._operation_results[(
                kind, str(table_scope), str(partition_scope), str(operation_id)
            )] = record
            if kind == "merge":
                self._merge_results[str(operation_id)] = record
            elif kind == "compact":
                self._compact_results[str(operation_id)] = record
        return record

    def get_operation_result(
        self,
        operation_id: str,
        kind: str | None = None,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any] | None:
        op_id = str(operation_id)
        if kind is not None and lake_table is not None:
            return self._operation_results.get((
                kind,
                str(lake_table),
                str(partition_key or ""),
                op_id,
            ))
        if kind is not None:
            return self._operation_results.get((kind, op_id))
        if lake_table is not None:
            scoped_kind = "merge" if op_id in self._merge_results else "compact" if op_id in self._compact_results else None
            if scoped_kind is not None:
                scoped = self._operation_results.get((
                    scoped_kind, str(lake_table), str(partition_key or ""), op_id
                ))
                if scoped is not None:
                    return scoped
        if (merge := self._merge_results.get(op_id)) is not None:
            return merge
        if (compact := self._compact_results.get(op_id)) is not None:
            return compact
        for key, record in self._operation_results.items():
            if key[-1] == op_id:
                return record
        return None

    def save_merge_result(
        self,
        merge_id: str,
        result: dict[str, Any],
        *,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any]:
        return self.save_operation_result(
            "merge", merge_id, result,
            lake_table=lake_table, partition_key=partition_key,
        )

    def get_merge_result(
        self,
        merge_id: str,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any] | None:
        return self.get_operation_result(
            merge_id, "merge", lake_table=lake_table, partition_key=partition_key
        )

    def save_compact_result(
        self,
        compact_id: str,
        result: dict[str, Any],
        *,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any]:
        return self.save_operation_result(
            "compact", compact_id, result,
            lake_table=lake_table, partition_key=partition_key,
        )

    def get_compact_result(
        self,
        compact_id: str,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any] | None:
        return self.get_operation_result(
            compact_id, "compact", lake_table=lake_table, partition_key=partition_key
        )

    def publish_manifest(
        self,
        lake_table: str | None = None,
        partition_key: str | None = None,
        old_file_ids: list[str] | None = None,
        new_files: list[PartitionFile] | None = None,
        expected_generation: int | None = None,
        *args: Any,
        **kwargs: Any,
    ) -> dict[str, Any]:
        """存储层发布入口；实际共用编排放 ManifestPublisher。"""
        from app.services.manifest_publisher import ManifestPublisher

        if old_file_ids is None:
            old_file_ids = kwargs.pop("input_file_ids", [])
        if new_files is None:
            new_files = kwargs.pop("output_files", [])
        if expected_generation is None:
            expected_generation = kwargs.pop("expected_version", None)
        if expected_generation is None:
            expected_generation = kwargs.pop("base_generation", None)
        if args:
            # 兼容可能的位置参数顺序：old_ids,new_files,generation。
            if not old_file_ids and isinstance(args[0], list):
                old_file_ids = args[0]
            if len(args) >= 2 and isinstance(args[1], list):
                new_files = args[1]
            if len(args) >= 3 and expected_generation is None:
                expected_generation = args[2]
        return ManifestPublisher(self).publish(
            lake_table=str(lake_table),
            partition_key=str(partition_key),
            input_file_ids=list(old_file_ids or []),
            output_files=list(new_files or []),
            expected_generation=expected_generation,
            **kwargs,
        )

    commit_manifest = publish_manifest
    replace_partition_manifest = publish_manifest

    def verify_publication(self, publication_id: str) -> dict[str, Any] | None:
        pub = self.get_publication(publication_id)
        if pub is None:
            return None
        table = pub["lake_table"]
        partition = pub["partition_key"]
        snap = self.get_partition_manifest(table, partition)
        current_ids = set(snap["file_ids"])
        output_ids = list(pub.get("output_file_ids", []))
        input_ids = list(pub.get("input_file_ids", []))
        return {
            "publication_id": publication_id,
            "operation_type": pub.get("operation_type"),
            "operation_id": pub.get("operation_id"),
            "lake_table": table,
            "partition_key": partition,
            "base_generation": pub.get("base_generation"),
            "committed_generation": pub.get("committed_generation"),
            "current_generation": snap["generation"],
            "generation_current": pub.get("committed_generation") == snap["generation"],
            "input_file_ids": input_ids,
            "output_file_ids": output_ids,
            "current_file_ids": snap["file_ids"],
            "output_still_manifest": all(fid in current_ids for fid in output_ids),
            "active_output_file_ids": [fid for fid in output_ids if fid in current_ids],
            "superseded_output_file_ids": [fid for fid in output_ids if fid not in current_ids],
            "inputs_still_manifest": [fid for fid in input_ids if fid in current_ids],
            "missing_input_file_ids": [fid for fid in input_ids if fid not in current_ids],
            "manifest_checksum": snap["manifest_checksum"],
        }

    # ---- 快照操作 ----

    def create_snapshot(
        self,
        lake_table: str,
        file_ids: list[str],
        parent_snapshot: str | None = None,
        added_rows: int = 0,
        deleted_rows: int = 0,
    ) -> LakeSnapshot:
        snap_id = f"snap-{lake_table}-{uuid.uuid4().hex[:8]}"
        manifest = hashlib.md5(
            ",".join(sorted(file_ids)).encode()
        ).hexdigest()
        snap = LakeSnapshot(
            snapshot_id=snap_id,
            lake_table=lake_table,
            parent_snapshot=parent_snapshot,
            file_ids=list(file_ids),
            added_rows=added_rows,
            deleted_rows=deleted_rows,
            manifest_checksum=manifest,
            status="generating",
            created_at=time.time(),
        )
        chain = self._snapshots.setdefault(lake_table, [])
        chain.append(snap)
        return snap

    def mark_snapshot_available(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "available"
                return snap
        return None

    def mark_snapshot_expired(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                snap.status = "expired"
                return snap
        return None

    def get_snapshot(self, lake_table: str, snapshot_id: str) -> LakeSnapshot | None:
        for snap in self._snapshots.get(lake_table, []):
            if snap.snapshot_id == snapshot_id:
                return snap
        return None

    def list_snapshots(self, lake_table: str) -> list[LakeSnapshot]:
        return list(self._snapshots.get(lake_table, []))

    def get_snapshot_at_time(self, lake_table: str, timestamp: float) -> LakeSnapshot | None:
        """时间旅行：返回 <=timestamp 的最近可用快照。"""
        candidates = [
            s for s in self._snapshots.get(lake_table, [])
            if s.status == "available" and s.created_at <= timestamp
        ]
        if not candidates:
            return None
        return max(candidates, key=lambda s: s.created_at)

    # ---- 水位操作 ----

    def set_watermark(
        self, lake_table: str, field_name: str, committed: str,
        strategy: str = "append"
    ) -> WatermarkRecord:
        wm = WatermarkRecord(
            lake_table=lake_table,
            field_name=field_name,
            committed_value=committed,
            pending_value=None,
            strategy=strategy,
            last_advanced_at=time.time(),
        )
        self._watermarks[lake_table] = wm
        return wm

    def advance_watermark(self, lake_table: str, new_value: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None:
            return None
        wm.pending_value = new_value
        return wm

    def commit_watermark(self, lake_table: str) -> WatermarkRecord | None:
        wm = self._watermarks.get(lake_table)
        if wm is None or wm.pending_value is None:
            return None
        wm.committed_value = wm.pending_value
        wm.pending_value = None
        wm.last_advanced_at = time.time()
        return wm

    def get_watermark(self, lake_table: str) -> WatermarkRecord | None:
        return self._watermarks.get(lake_table)

    def list_watermarks(self) -> list[WatermarkRecord]:
        return list(self._watermarks.values())

    # ---- 批次数据 ----

    def store_batch(
        self, batch_id: str, lake_table: str, source_system: str,
        rows: list[dict[str, Any]], window_start: str, window_end: str,
    ) -> BatchRecord:
        checksum = hashlib.md5(
            str(sorted([str(r) for r in rows])).encode()
        ).hexdigest()
        br = BatchRecord(
            batch_id=batch_id,
            lake_table=lake_table,
            source_system=source_system,
            rows=list(rows),
            row_count=len(rows),
            window_start=window_start,
            window_end=window_end,
            checksum=checksum,
            snapshot_id=None,
            status="pending",
            created_at=time.time(),
        )
        self._batches[batch_id] = br
        return br

    def get_batch(self, batch_id: str) -> BatchRecord | None:
        return self._batches.get(batch_id)

    def list_batches(self, lake_table: str) -> list[BatchRecord]:
        return [b for b in self._batches.values() if b.lake_table == lake_table]

    def update_batch_status(self, batch_id: str, status: str, snapshot_id: str | None = None) -> BatchRecord | None:
        br = self._batches.get(batch_id)
        if br is None:
            return None
        br.status = status
        if snapshot_id is not None:
            br.snapshot_id = snapshot_id
        return br

    # ---- 隔离区 ----

    def quarantine_rows(
        self, batch_id: str, rule_id: str, rule_name: str,
        rows: list[dict[str, Any]]
    ) -> list[dict[str, Any]]:
        """将被质量规则拦截的行放入隔离区。"""
        quarantined = []
        for row in rows:
            qr = dict(row)
            qr["_quarantine_batch"] = batch_id
            qr["_quarantine_rule"] = rule_id
            qr["_quarantine_rule_name"] = rule_name
            qr["_quarantine_time"] = time.time()
            quarantined.append(qr)
        self._quarantine.setdefault(batch_id, []).extend(quarantined)
        return quarantined

    def list_quarantine(self, batch_id: str | None = None) -> list[dict[str, Any]]:
        if batch_id is not None:
            return list(self._quarantine.get(batch_id, []))
        result: list[dict[str, Any]] = []
        for rows in self._quarantine.values():
            result.extend(rows)
        return result

    def release_quarantine(self, batch_id: str) -> list[dict[str, Any]]:
        """回炉重入：从隔离区取出数据行。"""
        rows = self._quarantine.pop(batch_id, [])
        cleaned: list[dict[str, Any]] = []
        for r in rows:
            cleaned_row = {k: v for k, v in r.items() if not k.startswith("_quarantine_")}
            cleaned.append(cleaned_row)
        return cleaned

    # ---- 归并检查点 ----

    def save_merge_checkpoint(
        self,
        merge_id: str,
        partial_file_ids: list[str],
        lake_table: str | dict[str, Any] | None = None,
        partition_key: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any]:
        # 兼容第三种位置参数直接传检查点元数据字典的写法。
        metadata: dict[str, Any] | None = None
        if isinstance(lake_table, dict):
            metadata = dict(lake_table)
            lake_table = metadata.pop("lake_table", metadata.pop("table", None))
            partition_key = partition_key or metadata.pop(
                "partition_key", metadata.pop("partition", None)
            )
        with self._manifest_lock:
            checkpoint = dict(self._merge_checkpoints.get(merge_id, {}))
            if metadata:
                checkpoint.update(metadata)
            checkpoint.update(kwargs)
            for generation_key, version_key in (
                ("base_generation", "base_version"),
                ("generation_at_start", "version_at_start"),
                ("current_generation", "current_version"),
            ):
                if generation_key in checkpoint:
                    checkpoint.setdefault(version_key, checkpoint[generation_key])
            # 该键是既有测试和恢复逻辑依赖的稳定契约，必须保持为输入 ID 列表。
            checkpoint["partial_file_ids"] = list(partial_file_ids)
            checkpoint.setdefault("saved_at", time.time())
            checkpoint["updated_at"] = time.time()
            if lake_table is not None:
                checkpoint["lake_table"] = lake_table
            if partition_key is not None:
                checkpoint["partition_key"] = partition_key
            self._merge_checkpoints[merge_id] = checkpoint
            return copy.deepcopy(checkpoint)

    def update_merge_checkpoint(self, merge_id: str, **changes: Any) -> dict[str, Any] | None:
        with self._manifest_lock:
            cp = self._merge_checkpoints.get(merge_id)
            if cp is None:
                return None
            cp.update(changes)
            cp["updated_at"] = time.time()
            return copy.deepcopy(cp)

    def get_merge_checkpoint(self, merge_id: str) -> dict[str, Any] | None:
        cp = self._merge_checkpoints.get(merge_id)
        return copy.deepcopy(cp) if cp is not None else None

    def clear_merge_checkpoint(self, merge_id: str) -> None:
        with self._manifest_lock:
            self._merge_checkpoints.pop(merge_id, None)

    # ---- 并发写锁 ----

    def acquire_write_lock(self, lake_table: str) -> threading.Lock:
        return self._write_lock.acquire(lake_table)

    def release_write_lock(self, lake_table: str) -> None:
        self._write_lock.release(lake_table)

    # ---- 统计快照 ----

    def stats_snapshot(self, lake_table: str) -> dict[str, Any]:
        """生成湖表统计画像快照。"""
        files = self.list_files(lake_table)
        snapshots = self.list_snapshots(lake_table)
        wm = self.get_watermark(lake_table)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        partition_count = len(set(f.partition_key for f in files))
        return {
            "lake_table": lake_table,
            "file_count": len(files),
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "partition_count": partition_count,
            "available_snapshots": sum(1 for s in snapshots if s.status == "available"),
            "watermark": wm.committed_value if wm else None,
        }


# 全局单例
lake_store = LakeStore()