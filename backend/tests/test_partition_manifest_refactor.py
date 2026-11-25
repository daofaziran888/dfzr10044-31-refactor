"""分区清单代次、归并恢复和压实重叠提交的回归测试。"""
from __future__ import annotations

import pytest

from app.lake_store import LakeStore
from app.services.compact_job import CompactJobService
from app.services.small_file_merge import SmallFileMergeService


@pytest.fixture
def store() -> LakeStore:
    return LakeStore()


def _rows(store: LakeStore, table: str, partition: str) -> int:
    return sum(f.file_rows for f in store.list_files(table, partition))


def test_normal_merge_only_replaces_selected_files(store: LakeStore) -> None:
    table, partition = "manifest_normal", "20261125"
    a = store.register_file(table, partition, 3, 300)
    b = store.register_file(table, partition, 4, 400)
    c = store.register_file(table, partition, 2, 200)

    result = SmallFileMergeService(store).execute_merge_atomic(
        table, partition, [a.file_id, b.file_id], "merge-normal"
    )

    assert result["merge_id"] == "merge-normal"
    assert result["result_rows"] == 7
    assert result["result_bytes"] > 0
    assert result["result_file"]
    active = store.list_files(table, partition)
    assert {f.file_id for f in active} == {c.file_id, result["result_file"]}
    assert _rows(store, table, partition) == 9


def test_compact_wins_and_merge_recovery_locates_disposition(store: LakeStore) -> None:
    table, partition = "manifest_conflict", "20261125"
    a = store.register_file(table, partition, 3, 300)
    b = store.register_file(table, partition, 4, 400)
    c = store.register_file(table, partition, 2, 200)
    merge_svc = SmallFileMergeService(store)
    compact_svc = CompactJobService(store)

    # 模拟“临时结果写完、三参数清单替换前中断”。
    real_replace = store.replace_file_list

    def interrupt_before_replace(lake_table, old_file_ids, new_files):
        assert old_file_ids == [a.file_id, b.file_id]
        raise RuntimeError("stop before manifest replacement")

    store.replace_file_list = interrupt_before_replace  # type: ignore[method-assign]
    with pytest.raises(RuntimeError):
        merge_svc.execute_merge_atomic(
            table, partition, [a.file_id, b.file_id], "merge-conflict"
        )
    store.replace_file_list = real_replace  # type: ignore[method-assign]

    # 临时结果不能进入下游清单，C 始终可读。
    active_during_interrupt = store.list_files(table, partition)
    assert {f.file_id for f in active_during_interrupt} == {
        a.file_id,
        b.file_id,
        c.file_id,
    }
    assert _rows(store, table, partition) == 9
    checkpoint = store.get_merge_checkpoint("merge-conflict")
    assert checkpoint is not None
    assert checkpoint["partial_file_ids"] == [a.file_id, b.file_id]

    # 压实读取整个分区并先发布新清单。
    compact = compact_svc.execute_compact(table, partition, "compact-1")
    assert compact["read_files"] == 3
    assert compact["output_files"] == 1
    assert compact["total_rows"] == 9
    assert _rows(store, table, partition) == 9

    # 恢复归并时不得再次发布 A+B 的 7 行结果。
    recovery = merge_svc.recover_from_checkpoint("merge-conflict", table, partition)
    assert recovery is not None
    assert recovery["merge_id"] == "merge-conflict"
    assert recovery["recovered"] is True
    assert recovery["committed"] is False
    assert recovery["superseded"] is True
    assert recovery["compaction_id"] == "compact-1"

    active = store.list_files(table, partition)
    assert len(active) == 1
    assert active[0].file_id == compact["result_file"]
    assert _rows(store, table, partition) == 9

    located = merge_svc.get_merge_result("merge-conflict")
    assert located is not None
    assert located["compaction_id"] == "compact-1"
    assert located["located_result"]["result_file"] == compact["result_file"]

    # 同一压实申请重复提交，返回首次成功结果。
    repeated = compact_svc.execute_compact(table, partition, "compact-1")
    assert repeated["idempotent_replay"] is True
    assert repeated["result_file"] == compact["result_file"]
    assert _rows(store, table, partition) == 9

    # 旧 A/B 不能作为新归并输入再次产生 7 行文件。
    stale = merge_svc.execute_merge_atomic(
        table, partition, [a.file_id, b.file_id], "merge-stale"
    )
    assert stale["committed"] is False
    assert stale["superseded"] is True
    assert len(store.list_files(table, partition)) == 1
    assert _rows(store, table, partition) == 9


def test_merge_recovery_wins_and_stale_compact_does_not_overwrite(store: LakeStore) -> None:
    table, partition = "manifest_merge_wins", "20261125"
    a = store.register_file(table, partition, 3, 300)
    b = store.register_file(table, partition, 4, 400)
    c = store.register_file(table, partition, 2, 200)
    merge_svc = SmallFileMergeService(store)
    compact_svc = CompactJobService(store)

    with pytest.raises(RuntimeError):
        merge_svc.execute_merge_atomic(
            table,
            partition,
            [a.file_id, b.file_id],
            "merge-wins",
            interrupt_before_publish=True,
        )

    # 压实已经从旧代次读取整个分区，但尚未提交。
    prepared_compact = compact_svc.prepare_compact(table, partition, "compact-stale")
    assert prepared_compact["read_files"] == 3
    assert prepared_compact["base_generation"] == store.get_partition_generation(
        table, partition
    )

    recovery = merge_svc.recover_from_checkpoint("merge-wins", table, partition)
    assert recovery["committed"] is True
    assert recovery["result_rows"] == 7
    assert {f.file_id for f in store.list_files(table, partition)} == {
        recovery["result_file"],
        c.file_id,
    }

    checkpoint = store.get_merge_checkpoint("merge-wins")
    assert checkpoint is not None
    assert merge_svc.is_manifest_current("merge-wins", table) is False

    stale_commit = compact_svc.commit_compact(prepared_compact)
    assert stale_commit["committed"] is False
    assert stale_commit["conflict"] is True
    assert {f.file_id for f in store.list_files(table, partition)} == {
        recovery["result_file"],
        c.file_id,
    }
    assert _rows(store, table, partition) == 9


def test_adjacent_partitions_keep_their_own_manifests(store: LakeStore) -> None:
    table = "manifest_adjacent"
    p1, p2 = "20261125", "20261126"
    a = store.register_file(table, p1, 3, 300)
    b = store.register_file(table, p1, 4, 400)
    c = store.register_file(table, p1, 2, 200)
    d = store.register_file(table, p2, 8, 800)
    merge_svc = SmallFileMergeService(store)

    result = merge_svc.execute_merge_atomic(
        table, p1, [a.file_id, b.file_id], "merge-adjacent"
    )
    p1_files = store.list_files(table, p1)
    p2_files = store.list_files(table, p2)

    assert {f.file_id for f in p1_files} == {result["result_file"], c.file_id}
    assert [f.file_id for f in p2_files] == [d.file_id]
    assert _rows(store, table, p1) == 9
    assert _rows(store, table, p2) == 8
    assert store.get_partition_generation(table, p1) == 4
    assert store.get_partition_generation(table, p2) == 1
