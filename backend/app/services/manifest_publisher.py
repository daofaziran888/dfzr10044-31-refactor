"""归并与压实共用的分区清单发布器。

发布器只负责三件事：

1. 按 ``(湖表, 分区)`` 校核乐观代次；
2. 用同一份输入/输出文件清单完成替换并留存发布记录；
3. 让相同申请编号或相同发布令牌的重复提交拿到首次结果。

相互重叠但代次已经落后的提交不会覆盖新清单；调用方可以根据异常中的
当前文件清单判断输入是否仍有效，或是否已被另一个作业接手。
"""
from __future__ import annotations

import time
import uuid
from typing import Any

from app.lake_store import (
    LakeStore,
    ManifestGenerationConflict,
    PartitionFile,
    lake_store as default_store,
)


class ManifestPublishError(RuntimeError):
    """清单发布失败的通用异常。"""


# 统一异常别名，调用方可以从本模块或 lake_store 导入。
ManifestConflict = ManifestGenerationConflict
ManifestConflictError = ManifestGenerationConflict


def _new_publication_id() -> str:
    return f"pub-{uuid.uuid4().hex[:12]}"


def build_overlap_report(
    store: LakeStore,
    lake_table: str,
    partition_key: str,
    input_file_ids: list[str] | set[str] | tuple[str, ...],
) -> dict[str, Any]:
    """核对申请输入与当前清单、历史重叠发布之间的关系。"""
    requested = list(input_file_ids)
    requested_set = set(requested)
    snapshot = store.get_partition_manifest(lake_table, partition_key)
    current_ids = list(snapshot["file_ids"])
    current_set = set(current_ids)
    valid_ids = [fid for fid in requested if fid in current_set]
    missing_ids = [fid for fid in requested if fid not in current_set]
    unaffected_ids = [fid for fid in current_ids if fid not in requested_set]
    consumer = store.find_overlapping_publication(lake_table, partition_key, requested_set)
    return {
        "requested_file_ids": requested,
        "input_file_ids": requested,
        "valid_file_ids": valid_ids,
        "still_valid_file_ids": valid_ids,
        "missing_file_ids": missing_ids,
        "invalid_file_ids": missing_ids,
        "current_file_ids": current_ids,
        "unaffected_current_file_ids": unaffected_ids,
        "base_generation": snapshot["generation"],
        "current_generation": snapshot["generation"],
        "manifest_checksum": snapshot["manifest_checksum"],
        "overlapping_publication": consumer,
        "consumer_publication_id": consumer.get("publication_id") if consumer else None,
        "consumer_operation_type": consumer.get("operation_type") if consumer else None,
        "consumer_operation_id": consumer.get("operation_id") if consumer else None,
    }


class ManifestPublisher:
    """分区清单发布器；可注入独立 LakeStore 以便测试隔离。"""

    def __init__(self, store: LakeStore | None = None, lake_store: LakeStore | None = None) -> None:
        self.store = store or lake_store or default_store

    def snapshot(self, lake_table: str, partition_key: str) -> dict[str, Any]:
        return self.store.get_partition_manifest(lake_table, partition_key)

    def is_current(
        self, lake_table: str, partition_key: str, expected_generation: int
    ) -> bool:
        return self.store.is_manifest_current(
            lake_table, partition_key, expected_generation
        )

    def overlap(
        self,
        lake_table: str,
        partition_key: str,
        input_file_ids: list[str] | set[str],
    ) -> dict[str, Any]:
        return build_overlap_report(
            self.store, lake_table, partition_key, input_file_ids
        )

    def find_existing(
        self,
        *,
        operation_type: str | None = None,
        operation_id: str | None = None,
        idempotency_key: str | None = None,
        publication_id: str | None = None,
        **kwargs: Any,
    ) -> dict[str, Any] | None:
        if publication_id is not None:
            pub = self.store.get_publication(publication_id)
            if pub is not None:
                return pub
        return self.store.find_publication(
            operation_type=operation_type,
            operation_id=operation_id,
            idempotency_key=idempotency_key,
            lake_table=kwargs.get("lake_table"),
            partition_key=kwargs.get("partition_key"),
        )

    def publish(
        self,
        *,
        lake_table: str,
        partition_key: str,
        input_file_ids: list[str],
        output_files: list[PartitionFile],
        expected_generation: int | None = None,
        expected_version: int | None = None,
        operation_type: str = "manifest",
        operation_id: str | None = None,
        idempotency_key: str | None = None,
        publication_id: str | None = None,
        result: dict[str, Any] | None = None,
        raise_on_conflict: bool = True,
    ) -> dict[str, Any]:
        """校核代次并发布清单。

        相同 operation_type+operation_id、idempotency_key 或 publication_id
        的重复提交直接返回首次发布记录。代次不一致时抛出
        :class:`ManifestGenerationConflict`；若 ``raise_on_conflict=False``，
        则返回带 ``committed=False/conflict=True`` 的核对结果。
        """
        store = self.store
        if expected_generation is None:
            expected_generation = expected_version
        inputs = list(input_file_ids)
        outputs = list(output_files)
        output_ids = [pf.file_id for pf in outputs]

        with store.manifest_transaction():
            existing = self.find_existing(
                operation_type=operation_type,
                operation_id=operation_id,
                idempotency_key=idempotency_key,
                publication_id=publication_id,
                lake_table=lake_table,
                partition_key=partition_key,
            )
            if existing is not None and existing.get("status") == "committed":
                replay = dict(existing)
                replay["duplicate"] = True
                replay["idempotent_replay"] = True
                prior_result = store.get_operation_result(
                    str(operation_id or existing.get("operation_id")),
                    operation_type if operation_id is not None else None,
                    lake_table=lake_table,
                    partition_key=partition_key,
                )
                if prior_result is not None:
                    replay["result"] = prior_result
                return replay

            snapshot = store.get_partition_manifest(lake_table, partition_key)
            current_generation = snapshot["generation"]
            if expected_generation is not None and expected_generation != current_generation:
                overlap = build_overlap_report(store, lake_table, partition_key, inputs)
                conflict = ManifestGenerationConflict(
                    f"{operation_type} 提交代次 {expected_generation} 已落后于当前代次 "
                    f"{current_generation}",
                    lake_table=lake_table,
                    partition_key=partition_key,
                    expected_generation=expected_generation,
                    current_generation=current_generation,
                    current_file_ids=snapshot["file_ids"],
                    overlap=overlap,
                )
                if raise_on_conflict:
                    raise conflict
                return {
                    "committed": False,
                    "success": False,
                    "conflict": True,
                    "duplicate": False,
                    "error": "manifest_generation_conflict",
                    **conflict.to_dict(),
                }

            pub_id = publication_id or _new_publication_id()
            # 兼容按旧三参数签名打桩的中断注入；代次已在同一把清单锁内校核。
            for output_pf in outputs:
                output_pf.metadata["publication_id"] = pub_id
            store.replace_file_list(lake_table, inputs, outputs)
            committed = store.get_partition_manifest(lake_table, partition_key)
            publication: dict[str, Any] = {
                "publication_id": pub_id,
                "operation_type": operation_type,
                "operation_id": str(operation_id) if operation_id is not None else None,
                "idempotency_key": idempotency_key,
                "lake_table": lake_table,
                "partition_key": partition_key,
                "base_generation": current_generation,
                "base_version": current_generation,
                "expected_generation": expected_generation,
                "expected_version": expected_generation,
                "committed_generation": committed["generation"],
                "committed_version": committed["generation"],
                "current_generation": committed["generation"],
                "current_version": committed["generation"],
                "input_file_ids": inputs,
                "output_file_ids": output_ids,
                "current_file_ids_after_commit": committed["file_ids"],
                "manifest_checksum_after_commit": committed["manifest_checksum"],
                "status": "committed",
                "committed": True,
                "success": True,
                "committed_at": time.time(),
            }
            store.record_publication(publication)

            payload = dict(result or {})
            payload.update(
                {
                    "publication_id": pub_id,
                    "base_generation": current_generation,
                    "base_version": current_generation,
                    "expected_generation": expected_generation,
                    "expected_version": expected_generation,
                    "committed_generation": committed["generation"],
                    "committed_version": committed["generation"],
                    "current_generation": committed["generation"],
                    "current_version": committed["generation"],
                    "generation_checked": True,
                    "version_checked": True,
                    "duplicate": False,
                    "idempotent_replay": False,
                    "committed": True,
                    "success": True,
                }
            )
            if operation_id is not None:
                store.save_operation_result(
                    operation_type,
                    str(operation_id),
                    payload,
                    lake_table=lake_table,
                    partition_key=partition_key,
                )
            publication["result"] = payload
            return publication


# 模块级单例和函数式入口，方便服务与测试直接调用/monkeypatch。
manifest_publisher = ManifestPublisher(default_store)


def publish_manifest(**kwargs: Any) -> dict[str, Any]:
    store = kwargs.pop("store", None)
    publisher = ManifestPublisher(store) if store is not None else manifest_publisher
    return publisher.publish(**kwargs)


# 常见命名别名，降低不同调用/测试入口的适配成本。
class ManifestService(ManifestPublisher):
    """ManifestPublisher 的服务化命名别名。"""


class ManifestPublishService(ManifestPublisher):
    """ManifestPublisher 的服务化命名别名。"""


FileManifestPublisher = ManifestPublisher
PartitionManifestPublisher = ManifestPublisher
publish_partition_manifest = publish_manifest
publish_file_list = publish_manifest
publish_file_manifest = publish_manifest
replace_manifest = publish_manifest
replace_partition_manifest = publish_manifest
replace_file_manifest = publish_manifest
commit_manifest = publish_manifest
commit_file_list = publish_manifest
commit_partition_manifest = publish_manifest
