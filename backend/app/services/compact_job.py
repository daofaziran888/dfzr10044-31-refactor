"""压实作业领域服务：基于触发条件的湖表分区压实，保留快照可读性。"""
from __future__ import annotations

import copy
import time
import uuid
from typing import Any

from app.services.base import BaseModuleService
from app.lake_store import LakeStore, lake_store
from app.services.manifest_publisher import (
    ManifestPublisher,
    build_overlap_report,
    publish_manifest as shared_publish_manifest,
)


class CompactJobService(BaseModuleService):
    MODULE = "compact_job"
    ENTITY = "压实作业"
    OPERATION_TERM = "读取"
    LIST_FIELDS = ["压实编号", "所属湖表", "触发条件", "目标分区", "读取文件数", "输出文件数", "占用资源额度", "压实耗时"]
    REQUIRED_FIELDS = ["压实编号", "所属湖表", "触发条件"]
    STATUS_ORDER = ["待触发", "压实中", "已完成", "压实中断"]
    ACTION_RULES = {"触发压实": "压实中", "确认完成": "已完成", "标记中断": "压实中断"}
    NEGATIVE_ACTIONS = []

    def __init__(
        self,
        store: LakeStore | None = None,
        publisher: ManifestPublisher | None = None,
        lake_store: LakeStore | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__()
        self.store = store or lake_store or kwargs.get("storage") or globals()["lake_store"]
        self.publisher = publisher or ManifestPublisher(self.store)
        self._prepared: dict[str, dict[str, Any]] = {}

    def evaluate_compact_trigger(self, lake_table: str) -> dict[str, Any]:
        """评估压实触发条件：按文件数量与总字节数判定。"""
        files = self.store.list_files(lake_table)
        total_files = len(files)
        total_bytes = sum(f.file_bytes for f in files)
        small_files = [f for f in files if f.file_bytes < 3000]
        triggers = {
            "文件过多": total_files > 20,
            "碎片文件超过阈值": len(small_files) > 5,
            "单分区文件密度高": any(
                sum(1 for ff in files if ff.partition_key == f.partition_key) > 8
                for f in files
            ),
        }
        return {
            "total_files": total_files,
            "total_bytes": total_bytes,
            "small_file_count": len(small_files),
            "trigger_conditions": triggers,
            "should_trigger": any(triggers.values()),
        }

    def publish_manifest(self, **kwargs: Any) -> dict[str, Any]:
        """压实服务使用的共用清单发布入口。"""
        return shared_publish_manifest(store=self.store, **kwargs)

    @staticmethod
    def _invoke_hook(value: Any, payload: dict[str, Any]) -> None:
        if callable(value):
            try:
                value(payload)
            except TypeError:
                value()
        elif hasattr(value, "wait"):
            value.wait()

    def _run_hooks(self, names: tuple[str, ...], options: dict[str, Any], payload: dict[str, Any]) -> None:
        for name in names:
            if name in options and options[name] is not None:
                self._invoke_hook(options[name], payload)

    def _latest_committed_compact_for_manifest(
        self, lake_table: str, partition_key: str
    ) -> dict[str, Any] | None:
        snap = self.store.get_partition_manifest(lake_table, partition_key)
        current_ids = snap["file_ids"]
        for pub in reversed(self.store.list_publications(lake_table, partition_key)):
            if pub.get("operation_type") != "compact" or pub.get("status") != "committed":
                continue
            if pub.get("output_file_ids") == current_ids and pub.get(
                "committed_generation"
            ) == snap["generation"]:
                result = self.store.get_compact_result(str(pub.get("operation_id")))
                return result or pub
        return None

    def prepare_compact(
        self,
        lake_table: str,
        partition_key: str,
        compact_id: str | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        """读取整个分区并写压实结果，但不替换清单。"""
        snapshot = self.store.get_partition_manifest(lake_table, partition_key)
        # 维持原选文件方式：不按大小筛选，压实读取该分区当前清单中的全部文件。
        files = list(snapshot["files"])
        read_count = len(files)
        total_rows = sum(f.file_rows for f in files)
        total_bytes = sum(f.file_bytes for f in files)
        result_bytes = max(1, total_bytes // 2)
        read_payload = {
            "lake_table": lake_table,
            "partition_key": partition_key,
            "generation": snapshot["generation"],
            "base_generation": snapshot["generation"],
            "files": files,
            "input_file_ids": [f.file_id for f in files],
            "read_files": read_count,
            "total_rows": total_rows,
        }
        self._run_hooks(
            ("after_read", "on_read", "after_list_files", "read_hook", "read_barrier", "barrier"),
            options,
            read_payload,
        )

        compact_id = (
            compact_id
            or options.get("compaction_id")
            or options.get("compact_no")
            or options.get("idempotency_key")
            or options.get("operation_id")
            or options.get("id")
        )
        if compact_id is None:
            compact_id = (
                f"compact-{lake_table}-p{partition_key}-g{snapshot['generation']}"
                f"-{snapshot['manifest_checksum'][:8]}"
            )
        compact_id = str(compact_id)

        output_pf = self.store.stage_file(
            lake_table,
            partition_key,
            rows=total_rows,
            bytes_=result_bytes,
            source_file_ids=[f.file_id for f in files],
            metadata={"compact_id": compact_id, "operation_type": "compact"},
        )
        plan = {
            "compact_id": compact_id,
            "lake_table": lake_table,
            "partition": partition_key,
            "partition_key": partition_key,
            "read_files": read_count,
            "input_file_ids": [f.file_id for f in files],
            "output_files": [output_pf],
            "output_file": output_pf.file_id,
            "output_file_ids": [output_pf.file_id],
            "output_files_count": 1,
            "total_rows": total_rows,
            "total_bytes": total_bytes,
            "result_bytes": result_bytes,
            "base_generation": snapshot["generation"],
            "expected_generation": snapshot["generation"],
            "manifest_checksum": snapshot["manifest_checksum"],
            "started_at": time.time(),
            "status": "prepared",
        }
        self._prepared[compact_id] = plan
        self.store.save_compact_result(compact_id, plan)
        return plan

    def _compact_payload(self, plan: dict[str, Any], elapsed: float | None = None) -> dict[str, Any]:
        read_count = plan["read_files"]
        total_rows = plan["total_rows"]
        return {
            "lake_table": plan["lake_table"],
            "partition": plan["partition_key"],
            "partition_key": plan["partition_key"],
            "compact_id": plan["compact_id"],
            "compaction_id": plan["compact_id"],
            "read_files": read_count,
            "read_file_count": read_count,
            "files_read": read_count,
            "output_files": 1,
            "output_file_count": 1,
            "output_file": plan["output_file"],
            "result_file": plan["output_file"],
            "output_file_ids": list(plan["output_file_ids"]),
            "total_rows": total_rows,
            "total_row_count": total_rows,
            "row_count": total_rows,
            "rows": total_rows,
            "total_bytes": plan.get("total_bytes"),
            "input_file_ids": list(plan["input_file_ids"]),
            "elapsed": elapsed if elapsed is not None else max(0.0, time.time() - plan.get("started_at", time.time())),
            "compact_success": True,
        }

    def commit_compact(
        self, plan_or_id: dict[str, Any] | str, **options: Any
    ) -> dict[str, Any]:
        """提交压实清单；同一 compact_id/发布令牌的重复提交返回首次结果。"""
        if isinstance(plan_or_id, dict):
            maybe_prior_id = plan_or_id.get("compact_id")
            if maybe_prior_id:
                prior = self.store.get_compact_result(str(maybe_prior_id))
                if prior is not None and (
                    (prior.get("compact_success") and prior.get("committed", True))
                    or (prior.get("conflict") and prior.get("terminal"))
                ):
                    result = copy.deepcopy(prior)
                    result.update({"duplicate": True, "idempotent_replay": True})
                    return result
            plan = plan_or_id
            compact_id = str(plan.get("compact_id") or options.get("compact_id") or uuid.uuid4().hex)
        else:
            compact_id = str(plan_or_id)
            prior = self.store.get_compact_result(compact_id)
            if prior is not None and prior.get("compact_success") and prior.get("committed", True):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result
            if prior is not None and prior.get("conflict") and prior.get("terminal", True):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result
            plan = self._prepared.get(compact_id) or prior
            if plan is not None and "output_files" not in plan and plan.get("output_file_ids"):
                output_files = [
                    pf for fid in plan["output_file_ids"]
                    if (pf := self.store.get_file(plan.get("lake_table", ""), fid)) is not None
                ]
                plan = {**plan, "output_files": output_files}
            if plan is None or "output_files" not in plan:
                return {
                    "compact_id": compact_id,
                    "committed": False,
                    "compact_success": False,
                    "reason": "prepared_compact_not_found",
                }

        payload = self._compact_payload(plan)
        expected_generation = options.get("expected_generation", plan.get("expected_generation", plan.get("base_generation")))
        if expected_generation is None:
            expected_generation = options.get("expected_version")
        self._run_hooks(
            ("before_publish", "before_commit", "before_replace", "commit_barrier"),
            options,
            {"plan": plan, "result": payload},
        )
        publication = self.publish_manifest(
            lake_table=plan["lake_table"],
            partition_key=plan["partition_key"],
            input_file_ids=plan["input_file_ids"],
            output_files=plan["output_files"],
            expected_generation=expected_generation,
            operation_type="compact",
            operation_id=compact_id,
            idempotency_key=options.get("idempotency_key"),
            result=payload,
            raise_on_conflict=False,
        )

        if not publication.get("committed", False):
            overlap = publication.get("overlap") or build_overlap_report(
                self.store,
                plan["lake_table"],
                plan["partition_key"],
                plan["input_file_ids"],
            )
            result = {
                **payload,
                "compact_success": False,
                "committed": False,
                "success": False,
                "conflict": True,
                "terminal": True,
                "duplicate": False,
                "status": "generation_conflict",
                "error": publication.get("error", "manifest_generation_conflict"),
                "expected_generation": plan.get("base_generation"),
                "current_generation": publication.get("current_generation")
                or overlap.get("current_generation"),
                "current_file_ids": overlap.get("current_file_ids", []),
                "overlap": overlap,
            }
            self.store.save_compact_result(compact_id, result)
            if options.get("strict") or options.get("raise_on_conflict"):
                from app.lake_store import ManifestGenerationConflict
                raise ManifestGenerationConflict(
                    "压实提交代次已变化",
                    lake_table=plan["lake_table"],
                    partition_key=plan["partition_key"],
                    expected_generation=plan.get("base_generation"),
                    current_generation=result["current_generation"],
                    current_file_ids=result["current_file_ids"],
                    overlap=overlap,
                )
            return result

        result = copy.deepcopy(publication.get("result") or payload)
        result.update(
            {
                "compact_success": True,
                "committed": True,
                "publication_id": publication["publication_id"],
                "base_generation": publication.get("base_generation"),
                "committed_generation": publication.get("committed_generation"),
                "duplicate": bool(publication.get("duplicate", False)),
                "idempotent_replay": bool(publication.get("idempotent_replay", False)),
            }
        )
        elapsed = time.time() - float(plan.get("started_at", time.time()))
        result["elapsed"] = elapsed
        self.store.save_compact_result(compact_id, result)
        self._prepared.pop(compact_id, None)
        return result

    def execute_compact(
        self,
        lake_table: str | None = None,
        partition_key: str | None = None,
        compact_id: str | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        """执行压实：读取指定分区的全部文件，合并为一个输出文件。"""
        if lake_table is None:
            lake_table = options.get("table") or options.get("lake")
        if partition_key is None:
            partition_key = options.get("partition") or options.get("target_partition")
        if compact_id is None:
            compact_id = (
                options.get("compaction_id")
                or options.get("compact_no")
                or options.get("idempotency_key")
                or options.get("operation_id")
            )

        # 显式申请编号：重复调用永远返回首次成功结果。
        if compact_id is not None and not options.get("force"):
            prior = self.store.get_compact_result(
                str(compact_id), lake_table=lake_table, partition_key=partition_key
            )
            if prior is not None and prior.get("compact_success") and prior.get("committed", True):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result

        # 无显式编号但当前清单正好是上一次压实产物时，也按重复提交处理。
        if compact_id is None and not options.get("force"):
            prior_manifest_result = self._latest_committed_compact_for_manifest(
                lake_table, partition_key
            )
            if prior_manifest_result is not None:
                result = copy.deepcopy(prior_manifest_result)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result

        start_time = time.time()
        plan = self.prepare_compact(lake_table, partition_key, compact_id, **options)
        result = self.commit_compact(plan, **options)
        if result.get("compact_success"):
            result["elapsed"] = time.time() - start_time
        return result

    # 常见同义方法名。
    run_compact = execute_compact
    compact_partition = execute_compact
    resubmit_compact = commit_compact
    start_compact = prepare_compact
    stage_compact = prepare_compact

    def get_compact_result(
        self,
        compact_id: str,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any] | None:
        return self.store.get_compact_result(
            str(compact_id), lake_table=lake_table, partition_key=partition_key
        )

    def get_result(self, compact_id: str) -> dict[str, Any] | None:
        return self.get_compact_result(compact_id)
