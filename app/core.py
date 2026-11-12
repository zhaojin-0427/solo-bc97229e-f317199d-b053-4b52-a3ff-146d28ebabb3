"""划分核心逻辑。

导入采用两阶段提交：

1. 分析阶段（只读快照事务，不写正式库）：读取当前已确认状态，
   在内存中用并查集把"同工件号或同拍摄批次"的图片沿关联链连成组，
   计算出本次导入需要新建的组、需要合并的已有组以及合并后的分区继承结果。
   分析期间其他导入 / 指定分区操作可正常并发提交。

2. 提交阶段（短写事务）：先用 meta.state_version 与最新已确认状态核对，
   若版本不一致说明分析期间出现了新关联或分区变更，放弃本次计划并基于
   最新状态重新分析（有限次重试），从而保证不漏掉新关联、不覆盖分区。
"""
from __future__ import annotations

import hashlib
from collections import defaultdict
from dataclasses import dataclass

from .db import bump_version, connect, read_txn, state_version, write_txn

MAX_ATTEMPTS = 6  # 乐观并发校验失败时的最大重分析次数


@dataclass(frozen=True)
class ImageRow:
    image_id: str
    workpiece_no: str
    batch_no: str

    @property
    def content_hash(self) -> str:
        payload = f"{self.image_id}\x1f{self.workpiece_no}\x1f{self.batch_no}"
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


class ContentConflictError(Exception):
    """同图片 ID 但内容（工件号 / 拍摄批次）不一致。"""

    def __init__(self, image_id: str, existing: tuple[str, str], new: tuple[str, str]):
        super().__init__(image_id)
        self.image_id = image_id
        self.existing = existing  # (workpiece_no, batch_no)
        self.new = new


class PartitionConflictError(Exception):
    """导入会把两个不同分区的组连通，整批拒绝。"""

    def __init__(self, component: dict):
        super().__init__("partition conflict")
        self.component = component


class ImageNotFoundError(Exception):
    def __init__(self, image_id: str):
        super().__init__(image_id)
        self.image_id = image_id


class ConcurrentModificationError(Exception):
    """并发变更过于频繁，重试次数耗尽。"""


class _Retry(Exception):
    """内部信号：提交时状态版本已变化，需要基于最新状态重新分析。"""


class _UnionFind:
    def __init__(self) -> None:
        self.parent: dict[str, str] = {}

    def add(self, x: str) -> None:
        self.parent.setdefault(x, x)

    def find(self, x: str) -> str:
        parent = self.parent
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self.parent[rb] = ra


def _ph(items) -> str:
    return ",".join("?" * len(items))


# ---------------------------------------------------------------------------
# 导入：分析阶段（只读快照，不写库）
# ---------------------------------------------------------------------------

def _build_plan(conn, rows: list[ImageRow]) -> dict:
    """在只读快照上完成连通分析，返回提交计划。可能抛出内容/分区冲突。"""
    ids = [r.image_id for r in rows]
    existing = {
        row["image_id"]: row
        for row in conn.execute(
            f"SELECT image_id, workpiece_no, batch_no, content_hash, group_id "
            f"FROM images WHERE image_id IN ({_ph(ids)})",
            ids,
        )
    }

    # 同图片 ID：内容一致 -> 跳过不新增；内容不一致 -> 整批拒绝
    fresh: list[ImageRow] = []
    for r in rows:
        ex = existing.get(r.image_id)
        if ex is None:
            fresh.append(r)
        elif ex["content_hash"] != r.content_hash:
            raise ContentConflictError(
                r.image_id,
                (ex["workpiece_no"], ex["batch_no"]),
                (r.workpiece_no, r.batch_no),
            )

    if not fresh:
        return {"components": [], "insert_count": 0}

    # 与本次新图同工件号或同批次的全部已有图片（关联链的已有端点）
    wps = sorted({r.workpiece_no for r in fresh})
    bns = sorted({r.batch_no for r in fresh})
    related = conn.execute(
        f"SELECT image_id, workpiece_no, batch_no, group_id FROM images "
        f"WHERE workpiece_no IN ({_ph(wps)}) OR batch_no IN ({_ph(bns)})",
        wps + bns,
    ).fetchall()

    # 并查集：新图之间按同工件/同批次连通，再与已有图片连通
    uf = _UnionFind()
    by_wp: dict[str, list[str]] = defaultdict(list)
    by_bn: dict[str, list[str]] = defaultdict(list)
    for r in fresh:
        uf.add(r.image_id)
        by_wp[r.workpiece_no].append(r.image_id)
        by_bn[r.batch_no].append(r.image_id)
    for lst in list(by_wp.values()) + list(by_bn.values()):
        for other in lst[1:]:
            uf.union(lst[0], other)
    for rel in related:
        uf.add(rel["image_id"])
        for fid in by_wp.get(rel["workpiece_no"], ()):
            uf.union(fid, rel["image_id"])
        for fid in by_bn.get(rel["batch_no"], ()):
            uf.union(fid, rel["image_id"])

    # 汇总连通分量：新图 + 被波及的已有组
    comps: dict[str, dict] = {}
    for r in fresh:
        root = uf.find(r.image_id)
        comps.setdefault(root, {"fresh": [], "groups": set()})["fresh"].append(r)
    for rel in related:
        root = uf.find(rel["image_id"])
        if root in comps:  # related 必然与某张新图连通
            comps[root]["groups"].add(rel["group_id"])

    # 取涉及组的当前分区
    all_groups = sorted({g for c in comps.values() for g in c["groups"]})
    partitions: dict[int, str | None] = {}
    if all_groups:
        for row in conn.execute(
            f"SELECT group_id, partition FROM groups WHERE group_id IN ({_ph(all_groups)})",
            all_groups,
        ):
            partitions[row["group_id"]] = row["partition"]

    components = []
    for c in comps.values():
        parts = {partitions.get(g) for g in c["groups"]} - {None}
        if len(parts) > 1:
            # 连通了两个不同分区 -> 整批拒绝并说明冲突
            raise PartitionConflictError(
                _conflict_detail(conn, c, partitions)
            )
        partition = next(iter(parts)) if parts else None
        # 目标组：优先保留已带分区的组，否则取最小 group_id，保证结果确定
        existing_groups = sorted(c["groups"])
        target = next(
            (g for g in existing_groups if partitions.get(g) == partition and partition),
            existing_groups[0] if existing_groups else None,
        )
        components.append(
            {
                "fresh": c["fresh"],
                "target_group": target,
                "merge_groups": [g for g in existing_groups if g != target],
                "partition": partition,
            }
        )
    return {"components": components, "insert_count": len(fresh)}


def _conflict_detail(conn, component: dict, partitions: dict[int, str | None]) -> dict:
    groups = []
    for g in sorted(component["groups"]):
        samples = conn.execute(
            "SELECT image_id FROM images WHERE group_id = ? ORDER BY image_id LIMIT 5",
            (g,),
        ).fetchall()
        groups.append(
            {
                "group_id": g,
                "partition": partitions.get(g),
                "sample_images": [s["image_id"] for s in samples],
            }
        )
    return {
        "new_images": [r.image_id for r in component["fresh"]],
        "groups": groups,
        "partitions": sorted({p for p in partitions.values() if p}),
    }


# ---------------------------------------------------------------------------
# 导入：提交阶段（短写事务 + 版本核对）
# ---------------------------------------------------------------------------

def _apply_plan(conn, plan: dict) -> None:
    for comp in plan["components"]:
        target = comp["target_group"]
        if target is None:
            cur = conn.execute(
                "INSERT INTO groups (partition) VALUES (?)", (comp["partition"],)
            )
            target = cur.lastrowid
        else:
            for g in comp["merge_groups"]:
                conn.execute("UPDATE images SET group_id = ? WHERE group_id = ?", (target, g))
                conn.execute("DELETE FROM groups WHERE group_id = ?", (g,))
            if comp["partition"]:
                # 版本核对已通过，此处条件必成立；条件更新仅为纵深防御
                conn.execute(
                    "UPDATE groups SET partition = ? "
                    "WHERE group_id = ? AND (partition IS NULL OR partition = ?)",
                    (comp["partition"], target, comp["partition"]),
                )
        comp["resolved_group"] = target
        for r in comp["fresh"]:
            conn.execute(
                "INSERT INTO images (image_id, workpiece_no, batch_no, content_hash, group_id) "
                "VALUES (?, ?, ?, ?, ?)",
                (r.image_id, r.workpiece_no, r.batch_no, r.content_hash, target),
            )


def import_rows(rows: list[ImageRow]) -> dict:
    """导入一批图片目录信息。整批原子生效；冲突时整批拒绝。"""
    for _attempt in range(MAX_ATTEMPTS):
        conn = connect()
        try:
            # 阶段 1：只读快照上完成连通分析（不阻塞并发写入）
            with read_txn(conn):
                version = state_version(conn)
                plan = _build_plan(conn, rows)

            if plan["insert_count"] == 0:
                return {
                    "version": version,
                    "inserted": 0,
                    "skipped": len(rows),
                    "components": [],
                }

            # 阶段 2：短写事务提交，先与最新已确认状态核对版本
            try:
                with write_txn(conn):
                    if state_version(conn) != version:
                        raise _Retry  # 期间出现新关联 / 分区变更，重新分析
                    _apply_plan(conn, plan)
                    new_version = bump_version(conn)
            except _Retry:
                continue

            return {
                "version": new_version,
                "inserted": plan["insert_count"],
                "skipped": len(rows) - plan["insert_count"],
                "components": [
                    {
                        "group_id": c["resolved_group"],
                        "new_images": [r.image_id for r in c["fresh"]],
                        "merged_groups": c["merge_groups"],
                        "partition": c["partition"],
                    }
                    for c in plan["components"]
                ],
            }
        finally:
            conn.close()
    raise ConcurrentModificationError


# ---------------------------------------------------------------------------
# 指定分区
# ---------------------------------------------------------------------------

def assign_partition(image_id: str, partition: str) -> dict:
    """按任一图片 ID 把整组指定到 train / val。已指定的组不会被覆盖。"""
    conn = connect()
    try:
        with write_txn(conn):
            row = conn.execute(
                "SELECT i.group_id AS group_id, g.partition AS partition "
                "FROM images i JOIN groups g ON g.group_id = i.group_id "
                "WHERE i.image_id = ?",
                (image_id,),
            ).fetchone()
            if row is None:
                raise ImageNotFoundError(image_id)
            group_id = row["group_id"]
            final_partition = row["partition"]
            changed = False
            if final_partition is None:
                cur = conn.execute(
                    "UPDATE groups SET partition = ? "
                    "WHERE group_id = ? AND partition IS NULL",
                    (partition, group_id),
                )
                if cur.rowcount == 1:
                    changed = True
                    final_partition = partition
                    bump_version(conn)
                else:  # 防御：BEGIN IMMEDIATE 已串行化，理论上不会走到
                    final_partition = conn.execute(
                        "SELECT partition FROM groups WHERE group_id = ?", (group_id,)
                    ).fetchone()["partition"]
            count = conn.execute(
                "SELECT COUNT(*) AS c FROM images WHERE group_id = ?", (group_id,)
            ).fetchone()["c"]
            return {
                "image_id": image_id,
                "group_id": group_id,
                "partition": final_partition,
                "changed": changed,
                "images_in_group": count,
            }
    finally:
        conn.close()
