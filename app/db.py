"""SQLite 访问层：连接、事务与 schema 初始化。

一致性设计要点：
- WAL 模式：只读快照事务（连通分析）不阻塞并发写事务（导入提交 / 指定分区）。
- 所有状态变更都在单个写事务（BEGIN IMMEDIATE）内原子提交，
  失败回滚、进程重启后库中数据始终保持一致。
- meta.state_version 是全局状态版本号，每次已确认的变更（导入 / 指定分区）都会 +1，
  供导入提交时的乐观并发校验使用。
"""
from __future__ import annotations

import os
import sqlite3
from contextlib import contextmanager

DEFAULT_DB_PATH = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data", "app.db"
)


def db_path() -> str:
    return os.environ.get("DATABASE_PATH", DEFAULT_DB_PATH)


SCHEMA = """
CREATE TABLE IF NOT EXISTS groups (
    group_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    partition  TEXT CHECK (partition IN ('train', 'val') OR partition IS NULL),
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE IF NOT EXISTS images (
    image_id     TEXT PRIMARY KEY,
    workpiece_no TEXT NOT NULL,
    batch_no     TEXT NOT NULL,
    content_hash TEXT NOT NULL,
    group_id     INTEGER NOT NULL REFERENCES groups(group_id),
    created_at   TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
CREATE INDEX IF NOT EXISTS idx_images_workpiece ON images(workpiece_no);
CREATE INDEX IF NOT EXISTS idx_images_batch ON images(batch_no);
CREATE INDEX IF NOT EXISTS idx_images_group ON images(group_id);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value INTEGER NOT NULL
);
INSERT OR IGNORE INTO meta(key, value) VALUES ('state_version', 0);
"""


def connect() -> sqlite3.Connection:
    """新建连接（autocommit 模式，事务由 read_txn / write_txn 显式控制）。"""
    conn = sqlite3.connect(db_path(), timeout=30, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db() -> None:
    """幂等初始化 schema，可安全在每次启动时执行。"""
    path = db_path()
    directory = os.path.dirname(path)
    if directory:
        os.makedirs(directory, exist_ok=True)
    conn = connect()
    try:
        conn.executescript(SCHEMA)
    finally:
        conn.close()


@contextmanager
def read_txn(conn: sqlite3.Connection):
    """只读快照事务：连通分析与下载在此完成。

    WAL 模式下读快照期间，其他连接的导入 / 指定分区写事务可正常提交，
    本事务看到的始终是开始时刻的完整一致状态。
    """
    conn.execute("BEGIN")
    try:
        yield conn
    finally:
        conn.execute("ROLLBACK")  # 只读事务回滚即释放快照


@contextmanager
def write_txn(conn: sqlite3.Connection):
    """写事务：BEGIN IMMEDIATE 立即取得写锁，提交或回滚均为原子操作。"""
    conn.execute("BEGIN IMMEDIATE")
    try:
        yield conn
    except BaseException:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def state_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value FROM meta WHERE key = 'state_version'").fetchone()
    return int(row["value"])


def bump_version(conn: sqlite3.Connection) -> int:
    conn.execute("UPDATE meta SET value = value + 1 WHERE key = 'state_version'")
    return state_version(conn)
