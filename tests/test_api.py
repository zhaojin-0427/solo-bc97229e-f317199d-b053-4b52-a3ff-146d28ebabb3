"""端到端测试：分组、幂等、冲突、并发、失败回滚与重启一致性。"""
from __future__ import annotations

import csv
import io
import threading

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "test.db"))
    with TestClient(app, raise_server_exceptions=False) as c:
        yield c


def import_csv(client, rows: list[tuple[str, str, str]]):
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(["image_id", "workpiece_no", "batch_no"])
    writer.writerows(rows)
    return client.post(
        "/imports", files={"file": ("batch.csv", buf.getvalue().encode(), "text/csv")}
    )


def test_chain_grouping(client):
    # IMG001-IMG004 沿 工件号/批次 关联链连成一组；IMG005 独立成组
    r = import_csv(client, [
        ("IMG001", "WP-A", "LOT-1"),
        ("IMG002", "WP-A", "LOT-1"),
        ("IMG003", "WP-B", "LOT-1"),
        ("IMG004", "WP-B", "LOT-2"),
        ("IMG005", "WP-C", "LOT-3"),
    ])
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["inserted"] == 5
    assert len(body["components"]) == 2
    g1 = client.get("/images/IMG001").json()["group_id"]
    for img in ("IMG002", "IMG003", "IMG004"):
        assert client.get(f"/images/{img}").json()["group_id"] == g1
    assert client.get("/images/IMG005").json()["group_id"] != g1


def test_reimport_same_content_is_idempotent(client):
    rows = [("IMG001", "WP-A", "LOT-1"), ("IMG002", "WP-A", "LOT-2")]
    assert import_csv(client, rows).json()["inserted"] == 2
    r = import_csv(client, rows)
    assert r.status_code == 200
    assert r.json()["inserted"] == 0
    assert r.json()["skipped_duplicates"] == 2
    assert client.get("/stats").json()["images"] == 2


def test_same_id_different_content_rejected(client):
    assert import_csv(client, [("IMG001", "WP-A", "LOT-1")]).status_code == 200
    r = import_csv(client, [("IMG001", "WP-Z", "LOT-9"), ("IMG002", "WP-B", "LOT-1")])
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "content_conflict"
    # 整批拒绝：IMG002 也不能入库
    assert client.get("/images/IMG002").status_code == 404
    assert client.get("/stats").json()["images"] == 1


def test_empty_fields_rejected(client):
    payload = "image_id,workpiece_no,batch_no\nIMG001,,LOT-1\nIMG002,WP-B,\n"
    r = client.post("/imports", files={"file": ("b.csv", payload.encode(), "text/csv")})
    assert r.status_code == 400
    assert len(r.json()["detail"]["empty_fields"]) == 2
    assert client.get("/stats").json()["images"] == 0


def test_assign_by_any_image_id_covers_whole_group(client):
    import_csv(client, [
        ("IMG001", "WP-A", "LOT-1"),
        ("IMG002", "WP-A", "LOT-2"),
        ("IMG003", "WP-B", "LOT-2"),
    ])
    r = client.post("/partitions/assign", json={"image_id": "IMG003", "partition": "train"})
    assert r.status_code == 200
    assert r.json()["changed"] is True
    assert r.json()["images_in_group"] == 3
    for img in ("IMG001", "IMG002", "IMG003"):
        assert client.get(f"/images/{img}").json()["partition"] == "train"
    # 重复指定同一分区：幂等，不算改动
    r = client.post("/partitions/assign", json={"image_id": "IMG001", "partition": "train"})
    assert r.status_code == 200 and r.json()["changed"] is False
    # 改到另一分区：拒绝
    r = client.post("/partitions/assign", json={"image_id": "IMG002", "partition": "val"})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "already_assigned"
    assert client.get("/images/IMG001").json()["partition"] == "train"


def test_assign_unknown_image_404(client):
    assert client.post(
        "/partitions/assign", json={"image_id": "NOPE", "partition": "train"}
    ).status_code == 404


def test_new_images_inherit_group_partition(client):
    import_csv(client, [("IMG001", "WP-A", "LOT-1")])
    client.post("/partitions/assign", json={"image_id": "IMG001", "partition": "val"})
    r = import_csv(client, [("IMG002", "WP-A", "LOT-2"), ("IMG003", "WP-B", "LOT-2")])
    assert r.status_code == 200
    assert r.json()["components"][0]["partition"] == "val"
    assert client.get("/images/IMG003").json()["partition"] == "val"


def test_import_bridging_two_partitions_rejected(client):
    import_csv(client, [("IMG-T1", "WP-T", "LOT-T")])
    import_csv(client, [("IMG-V1", "WP-V", "LOT-V")])
    client.post("/partitions/assign", json={"image_id": "IMG-T1", "partition": "train"})
    client.post("/partitions/assign", json={"image_id": "IMG-V1", "partition": "val"})
    # 新图同时关联 train 组（WP-T）与 val 组（LOT-V）-> 整批拒绝
    r = import_csv(client, [("IMG-X", "WP-T", "LOT-V"), ("IMG-Y", "WP-Y", "LOT-9")])
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "partition_conflict"
    assert set(detail["component"]["partitions"]) == {"train", "val"}
    # 整批拒绝：两张新图都未入库，已有状态不变
    assert client.get("/images/IMG-X").status_code == 404
    assert client.get("/images/IMG-Y").status_code == 404
    stats = client.get("/stats").json()
    assert stats["images"] == 2


def test_unpartitioned_list_and_downloads(client):
    import_csv(client, [
        ("IMG001", "WP-A", "LOT-1"),
        ("IMG002", "WP-B", "LOT-2"),
        ("IMG003", "WP-C", "LOT-3"),
    ])
    client.post("/partitions/assign", json={"image_id": "IMG001", "partition": "train"})
    client.post("/partitions/assign", json={"image_id": "IMG002", "partition": "val"})

    un = client.get("/images/unpartitioned").json()
    assert un["count"] == 1
    assert un["images"][0]["image_id"] == "IMG003"

    train_csv = client.get("/downloads/train").text
    rows = list(csv.DictReader(io.StringIO(train_csv)))
    assert [r["image_id"] for r in rows] == ["IMG001"]
    assert rows[0]["partition"] == "train"

    val_csv = client.get("/downloads/val").text
    assert [r["image_id"] for r in csv.DictReader(io.StringIO(val_csv))] == ["IMG002"]

    un_csv = client.get("/downloads/unpartitioned").text
    assert [r["image_id"] for r in csv.DictReader(io.StringIO(un_csv))] == ["IMG003"]


def test_failed_import_rolls_back(client, monkeypatch):
    import_csv(client, [("IMG001", "WP-A", "LOT-1")])
    before = client.get("/stats").json()

    import app.core as core

    def boom(conn, plan):
        # 模拟提交阶段中途失败（已写入部分语句后抛错）
        conn.execute(
            "INSERT INTO images (image_id, workpiece_no, batch_no, content_hash, group_id) "
            "VALUES ('GHOST', 'X', 'X', 'x', 1)"
        )
        raise RuntimeError("simulated crash")

    monkeypatch.setattr(core, "_apply_plan", boom)
    r = import_csv(client, [("IMG002", "WP-B", "LOT-2")])
    assert r.status_code == 500
    after = client.get("/stats").json()
    assert after == before  # 图片关系与版本均未变
    assert client.get("/images/GHOST").status_code == 404


def test_restart_keeps_state(tmp_path, monkeypatch):
    db = tmp_path / "restart.db"
    monkeypatch.setenv("DATABASE_PATH", str(db))
    with TestClient(app) as c1:
        import_csv(c1, [("IMG001", "WP-A", "LOT-1"), ("IMG002", "WP-A", "LOT-2")])
        c1.post("/partitions/assign", json={"image_id": "IMG001", "partition": "train"})
        before_stats = c1.get("/stats").json()
        before_csv = c1.get("/downloads/train").text
    # 模拟重启：同一数据库文件重新初始化应用
    with TestClient(app) as c2:
        assert c2.get("/stats").json() == before_stats
        assert c2.get("/downloads/train").text == before_csv
        assert c2.get("/images/IMG002").json()["partition"] == "train"


def test_concurrent_imports_and_assigns(tmp_path, monkeypatch):
    """并发导入 + 指定分区后，图片关系与分区仍须一致。"""
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "concurrent.db"))
    with TestClient(app) as client:
        import_csv(client, [("IMG-T1", "WP-T", "LOT-T")])
        import_csv(client, [("IMG-V1", "WP-V", "LOT-V")])
        client.post("/partitions/assign", json={"image_id": "IMG-T1", "partition": "train"})
        client.post("/partitions/assign", json={"image_id": "IMG-V1", "partition": "val"})

        results: dict[str, list] = {"assign": []}

        def do_import(name, rows):
            results.setdefault(name, []).append(import_csv(client, rows))

        threads = [
            threading.Thread(target=do_import, args=("t1", [("IMG-A1", "WP-A", "LOT-T"), ("IMG-A2", "WP-A", "LOT-A")])),
            threading.Thread(target=do_import, args=("t2", [("IMG-B1", "WP-B", "LOT-V")])),
            threading.Thread(target=do_import, args=("t3", [("IMG-X", "WP-T", "LOT-V")])),  # 必冲突
            threading.Thread(target=do_import, args=("t4", [("IMG-T1", "WP-T", "LOT-T")])),  # 幂等重传
            threading.Thread(
                target=lambda: results["assign"].append(
                    client.post("/partitions/assign", json={"image_id": "IMG-A2", "partition": "train"})
                )
            ),
        ]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # 桥接 train 与 val 的导入必须被拒绝
        assert results["t3"][0].status_code == 409
        assert client.get("/images/IMG-X").status_code == 404
        # 幂等重传成功
        assert results["t4"][0].status_code == 200

        # 核心不变量：train 组与 val 组仍是不同的组，且各自分区正确
        g_train = client.get("/images/IMG-T1").json()
        g_val = client.get("/images/IMG-V1").json()
        assert g_train["partition"] == "train"
        assert g_val["partition"] == "val"
        assert g_train["group_id"] != g_val["group_id"]

        # 与 train 组连通的图片都必须在 train 组；与 val 连通的都在 val 组
        if client.get("/images/IMG-A1").status_code == 200:
            assert client.get("/images/IMG-A1").json()["group_id"] == g_train["group_id"]
            assert client.get("/images/IMG-A2").json()["group_id"] == g_train["group_id"]
        if client.get("/images/IMG-B1").status_code == 200:
            assert client.get("/images/IMG-B1").json()["group_id"] == g_val["group_id"]

        # 全库一致性：图片总数 = 基础 2 张 + 各成功导入的新图
        expected = 2
        expected += results["t1"][0].json()["inserted"]
        expected += results["t2"][0].json()["inserted"]
        assert client.get("/stats").json()["images"] == expected


def test_concurrent_assign_same_group_only_one_wins(tmp_path, monkeypatch):
    monkeypatch.setenv("DATABASE_PATH", str(tmp_path / "race.db"))
    with TestClient(app) as client:
        import_csv(client, [("IMG001", "WP-A", "LOT-1")])
        outcomes = []

        def assign(p):
            outcomes.append((p, client.post("/partitions/assign", json={"image_id": "IMG001", "partition": p}).status_code))

        threads = [threading.Thread(target=assign, args=(p,)) for p in ("train", "val")]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        codes = sorted(code for _, code in outcomes)
        assert codes == [200, 409]  # 一个成功，另一个因不可改动被拒
        final = client.get("/images/IMG001").json()["partition"]
        winner = [p for p, code in outcomes if code == 200][0]
        assert final == winner
