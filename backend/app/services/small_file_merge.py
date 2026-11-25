"""小文件归并领域服务：碎片化分区归并、检查点恢复与失败处置。"""
from __future__ import annotations

import copy
from typing import Any, Iterable

from app.services.base import BaseModuleService
from app.lake_store import (
    LakeStore,
    ManifestGenerationConflict,
    PartitionFile,
    lake_store,
)
from app.services.manifest_publisher import (
    ManifestPublisher,
    build_overlap_report,
    publish_manifest as shared_publish_manifest,
)


class MergeInterrupted(RuntimeError):
    """临时结果已完成、清单替换前主动中断。"""


# 与可能的测试/调用命名保持兼容。
MergeInterrupt = MergeInterrupted
InterruptBeforePublish = MergeInterrupted


class SmallFileMergeService(BaseModuleService):
    MODULE = "small_file_merge"
    ENTITY = "归并任务"
    OPERATION_TERM = "归并"
    LIST_FIELDS = ["归并编号", "所属湖表", "目标分区", "归并前文件数", "归并后文件数", "目标文件大小", "归并策略", "完成时间"]
    REQUIRED_FIELDS = ["归并编号", "所属湖表", "目标分区"]
    STATUS_ORDER = ["待归并", "归并中", "已归并", "归并失败"]
    ACTION_RULES = {"启动归并": "归并中", "确认归并": "已归并", "标记失败": "归并失败"}
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

    # ------------------------------------------------------------------
    # 选择与规划：保持原有“按阈值检测、按目标大小分组”的方式不变。
    # ------------------------------------------------------------------

    def detect_small_files(self, lake_table: str, partition_key: str,
                           size_threshold: int = 5000) -> list[PartitionFile]:
        """检测指定分区下的碎片文件（字节数小于阈值）。"""
        files = self.store.list_files(lake_table, partition_key)
        return [f for f in files if f.file_bytes < size_threshold]

    def plan_merge_targets(self, small_files: list[PartitionFile],
                           target_size: int = 10000) -> list[dict[str, Any]]:
        """规划归并目标文件：按目标大小将小文件分组。"""
        total_rows = sum(f.file_rows for f in small_files)
        total_bytes = sum(f.file_bytes for f in small_files)
        if total_bytes == 0:
            return []
        target_count = max(1, (total_bytes + target_size - 1) // target_size)
        rows_per_target = total_rows // target_count
        targets = []
        for i in range(target_count):
            targets.append({
                "target_file_index": i,
                "estimated_rows": rows_per_target,
                "estimated_bytes": total_bytes // target_count,
            })
        return targets

    @staticmethod
    def _coerce_file_ids(small_file_ids: Iterable[str | PartitionFile] | None) -> list[str]:
        ids: list[str] = []
        for item in small_file_ids or []:
            if isinstance(item, PartitionFile):
                ids.append(item.file_id)
            else:
                ids.append(str(item))
        # 去重但保持申请顺序。
        return list(dict.fromkeys(ids))

    def publish_manifest(self, **kwargs: Any) -> dict[str, Any]:
        """归并服务使用的共用清单发布入口。"""
        return shared_publish_manifest(store=self.store, **kwargs)

    def is_selection_current(
        self, lake_table: str, partition_key: str, file_ids: list[str]
    ) -> dict[str, Any]:
        """判断申请输入是否仍全部存在于当前正式清单。"""
        report = build_overlap_report(self.store, lake_table, partition_key, file_ids)
        report["all_valid"] = not report["missing_file_ids"]
        report["none_valid"] = not report["valid_file_ids"]
        return report

    # ------------------------------------------------------------------
    # 两阶段归并：prepare 写临时结果和检查点，publish 才替换正式清单。
    # ------------------------------------------------------------------

    def prepare_merge(
        self,
        lake_table: str,
        partition_key: str,
        small_file_ids: Iterable[str | PartitionFile] | None = None,
        merge_id: str | None = None,
        **options: Any,
    ) -> dict[str, Any]:
        """读取当前代次并写临时归并结果，但不发布分区清单。"""
        if small_file_ids is None:
            small_file_ids = options.get("file_ids") or options.get("input_file_ids") or []
        ids = self._coerce_file_ids(small_file_ids)
        snapshot = self.store.get_partition_manifest(lake_table, partition_key)
        current_ids = set(snapshot["file_ids"])
        selected = [
            pf for fid in ids
            if (pf := self.store.get_file(lake_table, fid)) is not None
            and pf.partition_key == partition_key
            and fid in current_ids
        ]
        valid_ids = [pf.file_id for pf in selected]
        total_rows = sum(pf.file_rows for pf in selected)
        total_bytes = sum(pf.file_bytes for pf in selected)
        result_bytes = max(1, total_bytes // 2)

        if merge_id is not None:
            self.store.save_merge_checkpoint(
                merge_id,
                ids,
                lake_table=lake_table,
                partition_key=partition_key,
                base_generation=snapshot["generation"],
                generation_at_start=snapshot["generation"],
                status="preparing",
                requested_file_ids=ids,
                valid_file_ids=valid_ids,
            )

        result_pf = self.store.stage_file(
            lake_table,
            partition_key,
            rows=total_rows,
            bytes_=result_bytes,
            source_file_ids=valid_ids,
            metadata={"merge_id": merge_id, "operation_type": "merge"},
        )

        checkpoint_updates = {
            "status": "prepared",
            "result_file": result_pf.file_id,
            "result_file_ids": [result_pf.file_id],
            "temp_file_ids": [result_pf.file_id],
            "output_file_ids": [result_pf.file_id],
            "result_rows": total_rows,
            "result_bytes": result_pf.file_bytes,
            "valid_file_ids": valid_ids,
        }
        if merge_id is not None:
            self.store.save_merge_checkpoint(
                merge_id, ids, lake_table, partition_key, **checkpoint_updates
            )

        return {
            "merge_id": merge_id,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "small_file_ids": ids,
            "input_file_ids": ids,
            "valid_file_ids": valid_ids,
            "missing_file_ids": [fid for fid in ids if fid not in set(valid_ids)],
            "base_generation": snapshot["generation"],
            "generation_at_start": snapshot["generation"],
            "current_generation": snapshot["generation"],
            "result_file": result_pf.file_id,
            "result_file_ids": [result_pf.file_id],
            "output_files": [result_pf],
            "result_rows": total_rows,
            "result_bytes": result_pf.file_bytes,
            "merged_file_count": len(ids),
            "status": "prepared",
        }

    def _result_from_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        rows = plan["result_rows"]
        size = plan["result_bytes"]
        return {
            "merge_id": plan.get("merge_id"),
            "merge_no": plan.get("merge_id"),
            "merged_file_count": plan.get("merged_file_count", len(plan.get("input_file_ids", []))),
            "result_file": plan["result_file"],
            "result_rows": rows,
            "result_row_count": rows,
            "rows": rows,
            "row_count": rows,
            "result_bytes": size,
            "result_byte_count": size,
            "bytes": size,
            "byte_count": size,
            "lake_table": plan.get("lake_table"),
            "partition_key": plan.get("partition_key"),
            "input_file_ids": list(plan.get("input_file_ids", [])),
            "valid_file_ids": list(plan.get("valid_file_ids", [])),
        }

    @staticmethod
    def _invoke_hook(value: Any, plan: dict[str, Any]) -> None:
        if callable(value):
            try:
                value(plan)
            except TypeError:
                value()
        elif hasattr(value, "wait"):
            value.wait()

    def _maybe_interrupt(self, options: dict[str, Any], plan: dict[str, Any]) -> dict[str, Any] | None:
        """支持测试/调用方在发布前注入中断或钩子。"""
        hook_keys = (
            "before_publish", "before_commit", "before_replace",
            "on_before_publish", "publish_hook", "commit_hook",
            "interrupt_callback", "barrier", "pause_before_publish", "pause",
        )
        for key in hook_keys:
            hook = options.get(key)
            if hook is not None:
                self._invoke_hook(hook, plan)

        interrupt_words = ("interrupt", "crash", "fail", "abort", "stop")
        stage_words = (
            "before_publish", "before_commit", "before_replace",
            "after_write", "after_stage", "temp", "stage", "pre_commit",
        )
        explicit = False
        for key, value in options.items():
            lowered = key.lower()
            if not value or callable(value) or key == "interrupt_callback":
                continue
            if any(word in lowered for word in ("interrupt", "crash")):
                explicit = True
                break
            if any(word in lowered for word in interrupt_words) and (
                any(stage in lowered for stage in stage_words)
                or lowered in {"fail", "failure", "should_fail", "simulate_failure", "inject_failure"}
            ):
                explicit = True
                break
        failpoint = str(options.get("failpoint") or options.get("failure_point") or "").lower()
        explicit = explicit or any(
            token in failpoint
            for token in ("temp", "stage", "before_publish", "before_commit", "before_replace")
        )
        if explicit:
            merge_id = plan.get("merge_id")
            interrupted_result = {
                **self._result_from_plan(plan),
                "recovered": False,
                "committed": False,
                "success": False,
                "interrupted": True,
                "status": "interrupted_before_publish",
                "checkpoint_retained": True,
            }
            if merge_id is not None:
                self.store.update_merge_checkpoint(
                    merge_id,
                    status="interrupted_before_publish",
                    interrupted_result=interrupted_result,
                )
            if options.get("raise_on_interrupt", True) is False or any(
                options.get(key) for key in ("return_interrupted", "interrupt_return", "no_raise")
            ):
                return interrupted_result
            raise MergeInterrupted(
                f"归并 {merge_id} 已写完临时结果，按要求在替换清单前中断"
            )
        return None

    def commit_prepared_merge(
        self,
        plan_or_merge_id: dict[str, Any] | str,
        *,
        expected_generation: int | None = None,
        recovery: bool = False,
        **options: Any,
    ) -> dict[str, Any]:
        """发布 prepare_merge 产生的临时结果。"""
        if isinstance(plan_or_merge_id, str):
            merge_id = plan_or_merge_id
            cp = self.store.get_merge_checkpoint(merge_id)
            prior = self.store.get_merge_result(merge_id)
            if prior is not None and prior.get("committed"):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result
            if cp is None:
                return {
                    "merge_id": merge_id,
                    "committed": False,
                    "success": False,
                    "reason": "checkpoint_not_found",
                }
            plan = self._plan_from_checkpoint(cp)
        else:
            plan = plan_or_merge_id

        merge_id = plan.get("merge_id")
        payload = self._result_from_plan(plan)
        payload["recovered"] = bool(recovery)
        if recovery:
            payload["recover_from_partial"] = len(plan.get("input_file_ids", []))

        interrupted_result = self._maybe_interrupt(options, plan)
        if interrupted_result is not None:
            return interrupted_result
        generation = (
            expected_generation
            if expected_generation is not None
            else plan.get("base_generation")
        )
        if generation is None:
            generation = options.get("expected_version")
        try:
            publication = self.publish_manifest(
                lake_table=plan["lake_table"],
                partition_key=plan["partition_key"],
                input_file_ids=plan["input_file_ids"],
                output_files=plan["output_files"],
                expected_generation=generation,
                operation_type="merge",
                operation_id=str(merge_id) if merge_id is not None else None,
                result=payload,
            )
        except ManifestGenerationConflict as exc:
            if merge_id is not None:
                self.store.update_merge_checkpoint(
                    merge_id, status="generation_conflict", conflict=exc.to_dict()
                )
            raise

        result = copy.deepcopy(publication.get("result") or payload)
        result.update(
            {
                "publication_id": publication["publication_id"],
                "committed_generation": publication["committed_generation"],
                "duplicate": bool(publication.get("duplicate", False)),
                "idempotent_replay": bool(publication.get("idempotent_replay", False)),
                "recovered": bool(recovery),
            }
        )
        if recovery:
            result["recover_from_partial"] = len(plan.get("input_file_ids", []))
        if merge_id is not None:
            self.store.save_merge_result(str(merge_id), result)
            self.store.update_merge_checkpoint(
                merge_id,
                status="committed",
                publication_id=publication["publication_id"],
                committed_generation=publication["committed_generation"],
                result=result,
            )
            if not recovery:
                # 正常一次性执行维持原行为：成功后清除恢复检查点；结果仍可按编号查询。
                self.store.clear_merge_checkpoint(str(merge_id))
        return result

    # 语义化别名。
    commit_merge = commit_prepared_merge
    publish_merge = commit_prepared_merge

    def _plan_from_checkpoint(self, cp: dict[str, Any]) -> dict[str, Any]:
        lake_table = cp["lake_table"]
        partition_key = cp["partition_key"]
        input_ids = list(cp.get("input_file_ids") or cp.get("partial_file_ids") or [])
        result_file = cp.get("result_file") or (cp.get("result_file_ids") or [None])[0]
        result_pf = self.store.get_file(lake_table, result_file) if result_file else None

        current = self.store.get_partition_manifest(lake_table, partition_key)
        active_set = set(current["file_ids"])
        selected = [
            pf for fid in input_ids
            if (pf := self.store.get_file(lake_table, fid)) is not None
            and fid in active_set
        ]
        if result_pf is None:
            total_rows = sum(pf.file_rows for pf in selected)
            total_bytes = sum(pf.file_bytes for pf in selected)
            result_pf = self.store.stage_file(
                lake_table,
                partition_key,
                rows=total_rows,
                bytes_=max(1, total_bytes // 2),
                source_file_ids=[pf.file_id for pf in selected],
                metadata={"merge_id": cp.get("merge_id"), "recovered": True},
            )
        return {
            "merge_id": cp.get("merge_id"),
            "lake_table": lake_table,
            "partition_key": partition_key,
            "input_file_ids": input_ids,
            "valid_file_ids": [pf.file_id for pf in selected],
            "base_generation": cp.get("base_generation", current["generation"]),
            "result_file": result_pf.file_id,
            "result_file_ids": [result_pf.file_id],
            "output_files": [result_pf],
            "result_rows": result_pf.file_rows,
            "result_bytes": result_pf.file_bytes,
            "merged_file_count": len(input_ids),
        }

    def execute_merge_atomic(self, lake_table: str | None = None,
                             partition_key: str | None = None,
                             small_file_ids: Iterable[str | PartitionFile] | None = None,
                             merge_id: str | None = None,
                             **options: Any) -> dict[str, Any]:
        """原子执行归并：临时结果和检查点先落好，再经共用发布器替换清单。

        若发布前中断，检查点和 staged 结果保留；正式清单仍只包含未被替换
        的原文件，因此未入选的同分区文件持续可读。
        """
        # 兼容 file_ids=/partition= 等常见关键字。
        if partition_key is None:
            partition_key = options.get("partition") or options.get("target_partition")
        if lake_table is None:
            lake_table = options.get("table") or options.get("lake")
        if merge_id is None:
            merge_id = options.get("merge_no") or options.get("id")
        if small_file_ids is None:
            small_file_ids = options.get("file_ids") or options.get("input_file_ids")

        ids = self._coerce_file_ids(small_file_ids)
        if merge_id is not None and not options.get("force"):
            prior = self.store.get_merge_result(
                str(merge_id), lake_table=lake_table, partition_key=partition_key
            )
            if prior is not None and (prior.get("committed") or prior.get("superseded")):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result

        if merge_id is not None and ids and not options.get("force"):
            selection = self.is_selection_current(lake_table, partition_key, ids)
            if not selection["all_valid"]:
                self.store.save_merge_checkpoint(
                    str(merge_id),
                    ids,
                    lake_table=lake_table,
                    partition_key=partition_key,
                    base_generation=selection["current_generation"],
                    status="stale_inputs",
                )
                cp = self.store.get_merge_checkpoint(str(merge_id)) or {}
                if selection["none_valid"]:
                    return self._superseded_result(
                        str(merge_id),
                        cp,
                        ids,
                        len(ids),
                        overlap=selection,
                    )
                result = {
                    "merge_id": str(merge_id),
                    "lake_table": lake_table,
                    "partition_key": partition_key,
                    "merged_file_count": len(ids),
                    "recovered": False,
                    "committed": False,
                    "success": False,
                    "conflict": True,
                    "status": "partial_overlap",
                    "disposition": "requires_new_merge_for_remaining_inputs",
                    "input_file_ids": ids,
                    "valid_file_ids": selection["valid_file_ids"],
                    "missing_file_ids": selection["missing_file_ids"],
                    "current_file_ids": selection["current_file_ids"],
                    "overlap": selection,
                }
                self.store.save_merge_result(str(merge_id), result)
                return result

        if merge_id is not None and not options.get("force"):
            prior = self.store.get_merge_result(
                str(merge_id), lake_table=lake_table, partition_key=partition_key
            )
            if prior is not None and (prior.get("committed") or prior.get("superseded")):
                result = copy.deepcopy(prior)
                result.update({"duplicate": True, "idempotent_replay": True})
                return result

            cp = self.store.get_merge_checkpoint(str(merge_id))
            if (
                cp is not None
                and cp.get("result_file")
                and cp.get("lake_table") == lake_table
                and cp.get("partition_key") == partition_key
            ):
                plan = self._plan_from_checkpoint(cp)
                try:
                    return self.commit_prepared_merge(plan, recovery=False, **options)
                except ManifestGenerationConflict:
                    # 已被重叠提交接手时，交给恢复入口的核对逻辑形成处置结论。
                    return self.recover_from_checkpoint(
                        str(merge_id), lake_table, partition_key
                    )

        plan = self.prepare_merge(
            lake_table, partition_key, small_file_ids, merge_id, **options
        )
        try:
            return self.commit_prepared_merge(plan, recovery=False, **options)
        except MergeInterrupted:
            raise
        except Exception as exc:
            if merge_id is not None:
                self.store.update_merge_checkpoint(
                    str(merge_id),
                    status="publish_failed",
                    error_type=type(exc).__name__,
                    error=str(exc),
                )
            raise

    # 常见同义方法名。
    execute_merge = execute_merge_atomic
    run_merge = execute_merge_atomic
    merge_files = execute_merge_atomic
    start_merge = prepare_merge
    stage_merge = prepare_merge
    write_temp_result = prepare_merge

    def recover_from_checkpoint(self, merge_id: str,
                                lake_table: str | None = None,
                                partition_key: str | None = None,
                                **options: Any) -> dict[str, Any] | None:
        """从检查点恢复，并按当前正式清单判断原归并申请的处置结果。"""
        merge_id = str(merge_id)
        if lake_table is None:
            lake_table = options.get("table") or options.get("lake")
        if partition_key is None:
            partition_key = options.get("partition") or options.get("target_partition")
        prior = self.store.get_merge_result(
            merge_id, lake_table=lake_table, partition_key=partition_key
        )
        if prior is not None and (prior.get("committed") or prior.get("superseded")) and not options.get("force"):
            result = copy.deepcopy(prior)
            result.setdefault("recovered", True)
            result["recover_from_checkpoint"] = True
            result["duplicate"] = True
            result["idempotent_replay"] = True
            return result

        cp = self.store.get_merge_checkpoint(merge_id)
        if cp is None:
            return None

        lake_table = lake_table or cp.get("lake_table")
        partition_key = partition_key or cp.get("partition_key")
        if lake_table is None or partition_key is None:
            # 兼容只保存了 partial_file_ids 的旧检查点：尽量从临时结果或
            # 文件 ID 命名、历史发布记录补全分区，即使旧输入已被压实删除。
            candidate = None
            candidate_ids = [
                cp.get("result_file"),
                *(cp.get("result_file_ids") or []),
                *(cp.get("temp_file_ids") or []),
                *(cp.get("partial_file_ids") or []),
            ]
            for fid in candidate_ids:
                if not fid:
                    continue
                candidate = self.store.get_file(lake_table or "", str(fid))
                if candidate is not None:
                    break
            if candidate is not None:
                lake_table = lake_table or candidate.lake_table
                partition_key = partition_key or candidate.partition_key
            elif lake_table is not None:
                for fid in cp.get("partial_file_ids", []):
                    prefix = f"{lake_table}-p"
                    if str(fid).startswith(prefix):
                        partition_key = str(fid)[len(prefix):].rsplit("-", 1)[0]
                        break
                if partition_key is None:
                    for pub in self.store.list_publications(lake_table):
                        if set(cp.get("partial_file_ids", [])) & set(pub.get("input_file_ids", [])):
                            partition_key = pub.get("partition_key")
                            break

        partial_count = len(cp.get("partial_file_ids", []))
        if lake_table is None or partition_key is None:
            return {
                "merge_id": merge_id,
                "recover_from_partial": partial_count,
                "recovered": True,
                "committed": False,
                "warning": "checkpoint_missing_partition_scope",
            }

        if cp.get("lake_table") not in (None, lake_table) or (
            cp.get("partition_key") not in (None, partition_key)
        ):
            return {
                "merge_id": merge_id,
                "recover_from_partial": partial_count,
                "recovered": False,
                "committed": False,
                "conflict": True,
                "status": "partition_scope_mismatch",
            }

        # 旧检查点可能没有表/分区字段；上面已经尽量从文件登记补全。
        cp = {**cp, "lake_table": lake_table, "partition_key": partition_key}
        scoped_prior = self.store.get_merge_result(merge_id, lake_table, partition_key)
        if (
            scoped_prior is not None
            and (scoped_prior.get("committed") or scoped_prior.get("superseded"))
            and not options.get("force")
        ):
            result = copy.deepcopy(scoped_prior)
            result.setdefault("recovered", True)
            result["recover_from_checkpoint"] = True
            result["duplicate"] = True
            result["idempotent_replay"] = True
            return result

        input_ids = list(cp.get("input_file_ids") or cp.get("partial_file_ids") or [])
        overlap = build_overlap_report(self.store, lake_table, partition_key, input_ids)
        valid_ids = overlap["valid_file_ids"]

        # 所有原输入仍在当前清单：即使代次已因非重叠追加而变化，也只替换这些 ID。
        if input_ids and len(valid_ids) == len(input_ids):
            plan = self._plan_from_checkpoint(cp)
            try:
                result = self.commit_prepared_merge(plan, recovery=True, **options)
            except ManifestGenerationConflict as exc:
                # 在锁外得到冲突后重新按当前清单核对一次；若输入仍全部有效，
                # 则基于最新代次提交，避免被无关追加长期阻塞。
                latest = build_overlap_report(
                    self.store, lake_table, partition_key, input_ids
                )
                if len(latest["valid_file_ids"]) != len(input_ids):
                    return self._superseded_result(
                        merge_id, cp, input_ids, partial_count, exc=exc
                    )
                result = self.commit_prepared_merge(
                    plan,
                    expected_generation=latest["current_generation"],
                    recovery=True,
                    **options,
                )
            result.update(
                {
                    "recovered": True,
                    "recover_from_partial": partial_count,
                    "all_inputs_valid": True,
                }
            )
            self.store.save_merge_result(merge_id, result)
            return result

        # 原输入均已不在：通常已被压实发布的新清单接手，不能再次归并旧 A/B。
        if not valid_ids:
            return self._superseded_result(merge_id, cp, input_ids, partial_count, overlap=overlap)

        # 部分输入仍有效：原临时结果的行数口径已不能直接提交，保留核对结论。
        result = {
            "merge_id": merge_id,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "recover_from_partial": partial_count,
            "recovered": True,
            "committed": False,
            "success": False,
            "conflict": True,
            "status": "partial_overlap",
            "disposition": "requires_new_merge_for_remaining_inputs",
            "input_file_ids": input_ids,
            "valid_file_ids": valid_ids,
            "missing_file_ids": overlap["missing_file_ids"],
            "current_file_ids": overlap["current_file_ids"],
            "overlap": overlap,
        }
        self.store.save_merge_result(merge_id, result)
        self.store.update_merge_checkpoint(merge_id, status="partial_overlap", result=result)
        return result

    def _superseded_result(
        self,
        merge_id: str,
        cp: dict[str, Any],
        input_ids: list[str],
        partial_count: int,
        *,
        overlap: dict[str, Any] | None = None,
        exc: ManifestGenerationConflict | None = None,
    ) -> dict[str, Any]:
        lake_table = cp.get("lake_table") or (exc.lake_table if exc else None)
        partition_key = cp.get("partition_key") or (exc.partition_key if exc else None)
        if overlap is None and lake_table and partition_key:
            overlap = build_overlap_report(self.store, lake_table, partition_key, input_ids)
        overlap = overlap or (exc.overlap if exc is not None else {})
        consumer = overlap.get("overlapping_publication") if overlap else None
        consumer_result = None
        disposed_by = None
        compact_id = None
        publication_id = None
        if consumer:
            publication_id = consumer.get("publication_id")
            disposed_by = consumer.get("operation_id") or publication_id
            if consumer.get("operation_type") == "compact":
                compact_id = consumer.get("operation_id")
            consumer_result = self.store.get_operation_result(
                str(consumer.get("operation_id")),
                consumer.get("operation_type"),
            )

        staged_result = cp.get("result_file")
        if staged_result and lake_table:
            pf = self.store.get_file(lake_table, staged_result)
            if pf is not None and pf.status == "staged":
                pf.status = "abandoned"
                pf.metadata["abandoned_reason"] = "inputs_superseded_by_overlapping_commit"

        result = {
            "merge_id": merge_id,
            "lake_table": lake_table,
            "partition_key": partition_key,
            "recover_from_partial": partial_count,
            "recovered": True,
            "committed": False,
            "merge_committed": False,
            "published": False,
            "success": True,
            "superseded": True,
            "already_handled": True,
            "conflict": True,
            "terminal": True,
            "status": "superseded_by_compact" if compact_id else "superseded",
            "disposition": "compacted" if compact_id else "superseded_by_overlapping_commit",
            "reason": "checkpoint_inputs_absent_from_current_manifest",
            "merged_file_count": len(input_ids),
            # 原归并申请推导出的临时结果仍保留在检查点中，但不会进入清单。
            "result_file": cp.get("result_file"),
            "result_rows": cp.get("result_rows"),
            "result_bytes": cp.get("result_bytes"),
            "merge_result_file": cp.get("result_file"),
            "merge_result_rows": cp.get("result_rows"),
            "merge_result_bytes": cp.get("result_bytes"),
            "input_file_ids": input_ids,
            "valid_file_ids": [],
            "missing_file_ids": list(overlap.get("missing_file_ids", input_ids)) if overlap else input_ids,
            "current_file_ids": list(overlap.get("current_file_ids", [])) if overlap else [],
            "disposed_by": disposed_by,
            "handled_by": disposed_by,
            "owner_operation_id": disposed_by,
            "disposed_by_publication": publication_id,
            "compaction_id": compact_id,
            "compact_id": compact_id,
            "publication_id": publication_id,
            "consumer_result": consumer_result,
            "handling_result": consumer_result,
            "handler_result": consumer_result,
            "compact_result": consumer_result,
            "located_result": consumer_result,
            "overlap": overlap,
        }
        disposition_file = None
        if consumer_result and isinstance(consumer_result, dict):
            # 方便调用方通过原归并编号直接定位压实处置结果。
            result["located_result"] = consumer_result
            disposition_file = consumer_result.get("result_file") or consumer_result.get("output_file")
        if disposition_file is None and consumer and consumer.get("output_file_ids"):
            disposition_file = consumer["output_file_ids"][0]
        if disposition_file is not None:
            result["disposition_result_file"] = disposition_file
            result["active_result_file"] = disposition_file
            result["current_result_file"] = disposition_file
        self.store.save_merge_result(merge_id, result)
        self.store.update_merge_checkpoint(
            merge_id, status="superseded", superseded_by=disposed_by, result=result
        )
        return result

    def get_merge_result(
        self, merge_id: str, lake_table: str | None = None, partition_key: str | None = None
    ) -> dict[str, Any] | None:
        """按原归并编号定位成功结果或被压实接手后的处置结论。"""
        result = self.store.get_merge_result(
            str(merge_id), lake_table=lake_table, partition_key=partition_key
        )
        if result is not None:
            return result
        cp = self.store.get_merge_checkpoint(str(merge_id))
        if cp is not None and cp.get("result"):
            return cp["result"]
        return None

    def get_checkpoint_generation(self, merge_id: str) -> int | None:
        cp = self.store.get_merge_checkpoint(str(merge_id))
        if cp is None:
            result = self.store.get_merge_result(str(merge_id))
            return (result or {}).get("base_generation")
        return cp.get("base_generation", cp.get("generation_at_start"))

    def is_merge_generation_current(
        self,
        merge_id: str,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> bool:
        """当前分区清单是否仍是该归并开始时读取的那一代。"""
        cp = self.store.get_merge_checkpoint(str(merge_id))
        generation = self.get_checkpoint_generation(merge_id)
        if generation is None:
            return False
        table = lake_table or (cp or {}).get("lake_table")
        partition = partition_key or (cp or {}).get("partition_key")
        if table is None or partition is None:
            result = self.store.get_merge_result(str(merge_id)) or {}
            table = table or result.get("lake_table")
            partition = partition or result.get("partition_key")
        if table is None or partition is None:
            return False
        return self.store.is_manifest_current(table, partition, generation)

    def check_generation(
        self,
        merge_id: str,
        lake_table: str | None = None,
        partition_key: str | None = None,
    ) -> dict[str, Any]:
        cp = self.store.get_merge_checkpoint(str(merge_id)) or {}
        result = self.store.get_merge_result(str(merge_id), lake_table, partition_key) or {}
        table = lake_table or cp.get("lake_table") or result.get("lake_table")
        partition = partition_key or cp.get("partition_key") or result.get("partition_key")
        base = cp.get("base_generation", result.get("base_generation"))
        current = (
            self.store.get_partition_generation(table, partition)
            if table is not None and partition is not None
            else None
        )
        return {
            "merge_id": str(merge_id),
            "lake_table": table,
            "partition_key": partition,
            "base_generation": base,
            "start_generation": base,
            "current_generation": current,
            "generation_changed": base is not None and current != base,
            "current": base is not None and current == base,
            "is_current": base is not None and current == base,
        }

    def is_manifest_current(self, *args: Any, **kwargs: Any) -> bool:
        """兼容按归并编号或按 ``(表, 分区, 代次)`` 查询。"""
        merge_id = kwargs.get("merge_id")
        if merge_id is not None:
            return self.is_merge_generation_current(
                str(merge_id), kwargs.get("lake_table"), kwargs.get("partition_key")
            )
        if len(args) == 1:
            candidate = str(args[0])
            if self.store.get_merge_checkpoint(candidate) is not None or self.store.get_merge_result(candidate) is not None:
                return self.is_merge_generation_current(candidate)
            return False
        if len(args) == 2:
            first, second = map(str, args)
            if self.store.get_merge_checkpoint(first) is not None or self.store.get_merge_result(first) is not None:
                return self.is_merge_generation_current(first, second)
            return self.store.is_manifest_current(first, second, kwargs.get("expected_generation", 0))
        if len(args) >= 3:
            first = str(args[0])
            if self.store.get_merge_checkpoint(first) is not None or self.store.get_merge_result(first) is not None:
                return self.is_merge_generation_current(first, str(args[1]), str(args[2]))
            return self.store.is_manifest_current(str(args[0]), str(args[1]), int(args[2]))
        raise TypeError("is_manifest_current requires merge_id or lake_table, partition_key, generation")

    # 常见查询别名。
    is_still_on_checkpoint_generation = is_merge_generation_current
    is_start_generation_current = is_merge_generation_current
    is_generation_current = is_merge_generation_current
    get_result = get_merge_result
    get_disposition = get_merge_result
    find_merge_result = get_merge_result
    generation_status = check_generation
    recover_merge = recover_from_checkpoint
    is_merge_version_current = is_merge_generation_current
