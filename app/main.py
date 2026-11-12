"""FastAPI 接口层：导入 CSV、指定分区、未分区列表与分区 CSV 下载。"""
from __future__ import annotations

import csv
import io
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import RedirectResponse, Response
from pydantic import BaseModel, Field

from . import core
from .db import connect, init_db, read_txn, state_version

Partition = Literal["train", "val"]

HEADER_ALIASES = {
    "image_id": {"image_id", "图片id"},
    "workpiece_no": {"workpiece_no", "工件号"},
    "batch_no": {"batch_no", "拍摄批次"},
}


@asynccontextmanager
async def lifespan(_app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="质检数据集划分服务",
    description=(
        "管理标注系统导出的图片目录信息（图片ID / 工件号 / 拍摄批次）。\n\n"
        "- 同工件号或同拍摄批次的图片沿关联链归为一组，整组进入同一分区；\n"
        "- 按任一图片 ID 将整组指定到 train / val，已指定不可改动；\n"
        "- 新图自动继承关联组分区；导入若连通两个不同分区则整批拒绝；\n"
        "- 同图片 ID 同内容重传幂等跳过，异内容整批拒绝。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


# ---------------------------------------------------------------------------
# 请求 / 响应模型
# ---------------------------------------------------------------------------

class AssignRequest(BaseModel):
    image_id: str = Field(min_length=1, description="组内任一图片 ID")
    partition: Partition = Field(description="目标分区：train 或 val")


class ComponentResult(BaseModel):
    group_id: int
    new_images: list[str]
    merged_groups: list[int]
    partition: str | None


class ImportResult(BaseModel):
    status: str
    rows_in_file: int
    inserted: int
    skipped_duplicates: int
    components: list[ComponentResult]
    state_version: int


class AssignResult(BaseModel):
    image_id: str
    group_id: int
    partition: str
    changed: bool = Field(description="本次调用是否真正写入了分区（False 表示组此前已有该分区）")
    images_in_group: int


# ---------------------------------------------------------------------------
# CSV 解析
# ---------------------------------------------------------------------------

def _http400(message: str, **extra) -> HTTPException:
    return HTTPException(status_code=400, detail={"error": "invalid_csv", "message": message, **extra})


def parse_csv(data: bytes) -> list[core.ImageRow]:
    """解析并校验 CSV。任何一行字段为空都整批拒绝。"""
    try:
        text = data.decode("utf-8-sig")
    except UnicodeDecodeError:
        raise _http400("CSV 必须是 UTF-8 编码（支持带 BOM）")

    rows = [r for r in csv.reader(io.StringIO(text)) if any(c.strip() for c in r)]
    if not rows:
        raise _http400("CSV 为空")

    header = [h.strip().lower() for h in rows[0]]
    col_index: dict[str, int] = {}
    for field_name, aliases in HEADER_ALIASES.items():
        for alias in aliases:
            if alias in header:
                col_index[field_name] = header.index(alias)
                break
    missing = [f for f in HEADER_ALIASES if f not in col_index]
    if missing:
        raise _http400(
            f"CSV 缺少必需列：{', '.join(missing)}",
            required_columns=["image_id", "workpiece_no", "batch_no"],
            accepted_aliases={k: sorted(v) for k, v in HEADER_ALIASES.items()},
        )

    # 收集所有空字段错误，一次性返回
    empty_errors = []
    for lineno, r in enumerate(rows[1:], start=2):
        for field_name, idx in col_index.items():
            value = r[idx].strip() if idx < len(r) else ""
            if not value:
                empty_errors.append({"line": lineno, "field": field_name})
    if empty_errors:
        raise _http400("存在空字段，图片ID / 工件号 / 拍摄批次均不能为空", empty_fields=empty_errors)

    # 文件内按图片 ID 去重：同内容去重，异内容整批拒绝
    seen: dict[str, tuple[str, str]] = {}
    for r in rows[1:]:
        image_id = r[col_index["image_id"]].strip()
        content = (r[col_index["workpiece_no"]].strip(), r[col_index["batch_no"]].strip())
        if image_id in seen and seen[image_id] != content:
            raise HTTPException(
                status_code=409,
                detail={
                    "error": "content_conflict",
                    "message": (
                        f"文件内图片 {image_id} 出现不一致的内容："
                        f"(工件号 {seen[image_id][0]}, 批次 {seen[image_id][1]}) 与 "
                        f"(工件号 {content[0]}, 批次 {content[1]})，整批已拒绝"
                    ),
                    "image_id": image_id,
                },
            )
        seen[image_id] = content

    if not seen:
        raise _http400("CSV 中没有数据行")
    return [core.ImageRow(image_id, wp, bn) for image_id, (wp, bn) in seen.items()]


# ---------------------------------------------------------------------------
# 路由
# ---------------------------------------------------------------------------

@app.get("/", include_in_schema=False)
def root():
    return RedirectResponse("/docs")


@app.get("/health", tags=["系统"], summary="健康检查")
def health():
    return {"status": "ok"}


@app.post(
    "/imports",
    tags=["导入"],
    summary="导入标注系统导出的 CSV",
    response_model=ImportResult,
    responses={
        400: {"description": "CSV 格式错误 / 存在空字段"},
        409: {"description": "同图片 ID 内容冲突，或导入会连通两个不同分区"},
        503: {"description": "并发变更过于频繁，请重试"},
    },
)
def import_csv(file: UploadFile = File(..., description="UTF-8 CSV，列：image_id, workpiece_no, batch_no")):
    rows = parse_csv(file.file.read())
    try:
        result = core.import_rows(rows)
    except core.ContentConflictError as e:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "content_conflict",
                "message": (
                    f"图片 {e.image_id} 已存在且内容不一致："
                    f"库中(工件号 {e.existing[0]}, 批次 {e.existing[1]})，"
                    f"新数据(工件号 {e.new[0]}, 批次 {e.new[1]})，整批已拒绝"
                ),
                "image_id": e.image_id,
                "existing": {"workpiece_no": e.existing[0], "batch_no": e.existing[1]},
                "incoming": {"workpiece_no": e.new[0], "batch_no": e.new[1]},
            },
        )
    except core.PartitionConflictError as e:
        groups_desc = "、".join(
            f"组 {g['group_id']}（{g['partition'] or '未分区'}）" for g in e.component["groups"]
        )
        raise HTTPException(
            status_code=409,
            detail={
                "error": "partition_conflict",
                "message": f"导入会把不同分区的组连通（{groups_desc}），整批已拒绝",
                "component": e.component,
            },
        )
    except core.ConcurrentModificationError:
        raise HTTPException(status_code=503, detail="并发变更过于频繁，请稍后重试本次导入")

    return ImportResult(
        status="ok",
        rows_in_file=len(rows),
        inserted=result["inserted"],
        skipped_duplicates=result["skipped"],
        components=result["components"],
        state_version=result["version"],
    )


@app.post(
    "/partitions/assign",
    tags=["分区"],
    summary="按任一图片 ID 将整组指定到 train / val",
    response_model=AssignResult,
    responses={
        404: {"description": "图片不存在"},
        409: {"description": "组已被指定到另一分区，不可改动"},
    },
)
def assign(req: AssignRequest):
    try:
        result = core.assign_partition(req.image_id, req.partition)
    except core.ImageNotFoundError:
        raise HTTPException(status_code=404, detail=f"图片 {req.image_id} 不存在")
    if not result["changed"] and result["partition"] != req.partition:
        raise HTTPException(
            status_code=409,
            detail={
                "error": "already_assigned",
                "message": (
                    f"图片 {req.image_id} 所在组 {result['group_id']} 已指定为 "
                    f"{result['partition']}，不可改动为 {req.partition}"
                ),
                **result,
            },
        )
    return AssignResult(**result)


@app.get("/images/unpartitioned", tags=["查询"], summary="未分区图片列表")
def unpartitioned():
    """返回所有所在组尚未指定分区的图片（供训练流水线跟进标注/划分）。"""
    conn = connect()
    try:
        with read_txn(conn):
            rows = conn.execute(
                "SELECT i.image_id, i.workpiece_no, i.batch_no, i.group_id "
                "FROM images i JOIN groups g ON g.group_id = i.group_id "
                "WHERE g.partition IS NULL ORDER BY i.image_id"
            ).fetchall()
            version = state_version(conn)
    finally:
        conn.close()
    return {
        "count": len(rows),
        "state_version": version,
        "images": [dict(r) for r in rows],
    }


@app.get(
    "/downloads/{partition}",
    tags=["查询"],
    summary="下载分区图片清单 CSV",
    responses={404: {"description": "分区名无效"}},
)
def download_partition(partition: Literal["train", "val", "unpartitioned"]):
    """下载指定分区（train / val / unpartitioned）的完整图片清单。

    每次下载都在单个只读快照事务中读取，保证拿到的是完整一致状态。
    """
    conn = connect()
    try:
        with read_txn(conn):
            if partition == "unpartitioned":
                rows = conn.execute(
                    "SELECT i.image_id, i.workpiece_no, i.batch_no, i.group_id, g.partition "
                    "FROM images i JOIN groups g ON g.group_id = i.group_id "
                    "WHERE g.partition IS NULL ORDER BY i.image_id"
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT i.image_id, i.workpiece_no, i.batch_no, i.group_id, g.partition "
                    "FROM images i JOIN groups g ON g.group_id = i.group_id "
                    "WHERE g.partition = ? ORDER BY i.image_id",
                    (partition,),
                ).fetchall()
            version = state_version(conn)
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["image_id", "workpiece_no", "batch_no", "group_id", "partition"])
    for r in rows:
        writer.writerow([r["image_id"], r["workpiece_no"], r["batch_no"], r["group_id"], r["partition"] or ""])
    return Response(
        content=buf.getvalue(),
        media_type="text/csv",
        headers={
            "Content-Disposition": f'attachment; filename="{partition}_partition.csv"',
            "X-State-Version": str(version),
        },
    )


@app.get("/images/{image_id}", tags=["查询"], summary="查询单张图片及其组信息")
def get_image(image_id: str):
    conn = connect()
    try:
        with read_txn(conn):
            row = conn.execute(
                "SELECT i.image_id, i.workpiece_no, i.batch_no, i.group_id, g.partition "
                "FROM images i JOIN groups g ON g.group_id = i.group_id "
                "WHERE i.image_id = ?",
                (image_id,),
            ).fetchone()
    finally:
        conn.close()
    if row is None:
        raise HTTPException(status_code=404, detail=f"图片 {image_id} 不存在")
    return dict(row)


@app.get("/groups", tags=["查询"], summary="全部连通组及其分区")
def list_groups():
    conn = connect()
    try:
        with read_txn(conn):
            rows = conn.execute(
                "SELECT g.group_id, g.partition, COUNT(i.image_id) AS image_count "
                "FROM groups g LEFT JOIN images i ON i.group_id = g.group_id "
                "GROUP BY g.group_id ORDER BY g.group_id"
            ).fetchall()
    finally:
        conn.close()
    return {"count": len(rows), "groups": [dict(r) for r in rows]}


@app.get("/stats", tags=["查询"], summary="总体统计与状态版本")
def stats():
    conn = connect()
    try:
        with read_txn(conn):
            images = conn.execute("SELECT COUNT(*) AS c FROM images").fetchone()["c"]
            groups = conn.execute("SELECT COUNT(*) AS c FROM groups").fetchone()["c"]
            by_partition = {
                r["partition"] or "unpartitioned": r["c"]
                for r in conn.execute(
                    "SELECT g.partition AS partition, COUNT(i.image_id) AS c "
                    "FROM groups g LEFT JOIN images i ON i.group_id = g.group_id "
                    "GROUP BY g.partition"
                )
            }
            version = state_version(conn)
    finally:
        conn.close()
    return {
        "images": images,
        "groups": groups,
        "images_by_partition": by_partition,
        "state_version": version,
    }
