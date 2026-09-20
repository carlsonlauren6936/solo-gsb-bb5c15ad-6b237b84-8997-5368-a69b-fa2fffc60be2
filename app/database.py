"""SQLite 持久化层。

四张表：
- chains：链条头信息与链尾缓存（tail_hash / current_holder 是事件表的可信投影）
- events：链上的全部事件，序号 0 为创世记录（文件摘要 + 首位持有人），
  序号 >=1 为移交记录。
- offers：待接收移交邀约。只保存一次性确认码的 SHA-256 摘要（code_digest），
  不保存明文；tail_hash 记录邀约发起时锁定的链尾，确认时必须仍然匹配。
  witness_threshold 为 NULL 表示无见证邀约（兼容存量数据），否则为通过阈值。
  cancel_code_digest 为可选撤销码的 SHA-256 摘要（绑定 offer_id 做域分隔），
  为 NULL 表示该邀约创建时未提供撤销码、不可撤销也不可替换（兼容存量邀约）；
  status 取值 pending / accepted / cancelled / replaced（rejected / expired /
  terminated 为派生状态，不额外改写邀约行），cancelled_at 记录撤销时刻；
  replaced_at / replaced_by 记录替换时刻与替换产生的新邀约 ID（自引用外键，
  新邀约继承原邀约锁定的同一链尾）。
- offer_witnesses：见证邀约的见证人。只保存一次性令牌的 SHA-256 摘要
  （token_digest，与 offer_id 做域分隔），不存明文；decision 为 NULL 表示
  尚未表态，同一见证人至多表态一次（令牌本身即一次性）。拒绝/过期/链尾推进
  等终止状态为派生状态，不额外改写邀约行。

防分叉保障：
1. 所有写操作在进程内串行锁 + SQLite `BEGIN IMMEDIATE` 中完成，
   追加前校验链尾哈希，校验与插入在同一事务内原子完成；
2. (chain_id, seq) 唯一、(chain_id, nonce) 唯一，由数据库兜底，
   任何竞争写入都不可能产生第二条分叉链或重放随机数。

建表语句全部为 CREATE ... IF NOT EXISTS，init_db() 可重复执行，
对已有数据库文件只做增量补表/补列/重建，不触碰存量数据。
"""
from __future__ import annotations

import sqlite3
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from . import config

# 进程内写锁：保证“读链尾 -> 校验 -> 追加提交”整体串行
_write_lock = threading.RLock()

# offers 表列定义（建表与旧表重建共用，保证两处结构一致）
_OFFERS_COLUMNS = """
    offer_id            TEXT PRIMARY KEY,
    chain_id            TEXT NOT NULL,
    tail_hash           TEXT NOT NULL,
    receiver            TEXT NOT NULL,
    code_digest         TEXT NOT NULL,
    witness_threshold   INTEGER,
    status              TEXT NOT NULL DEFAULT 'pending',
    expires_at          TEXT NOT NULL,
    created_at          TEXT NOT NULL,
    accepted_at         TEXT,
    accepted_seq        INTEGER,
    cancel_code_digest  TEXT,
    cancelled_at        TEXT,
    replaced_at         TEXT,
    replaced_by         TEXT,
    CHECK(status IN ('pending', 'accepted', 'cancelled', 'replaced')),
    CHECK(witness_threshold IS NULL OR witness_threshold >= 1),
    FOREIGN KEY(chain_id) REFERENCES chains(chain_id),
    FOREIGN KEY(replaced_by) REFERENCES offers(offer_id)
"""

_SCHEMA = f"""
CREATE TABLE IF NOT EXISTS chains (
    chain_id        TEXT PRIMARY KEY,
    file_hash       TEXT NOT NULL,
    first_holder    TEXT NOT NULL,
    tail_hash       TEXT NOT NULL,
    current_holder  TEXT NOT NULL,
    created_at      TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    chain_id      TEXT NOT NULL,
    seq           INTEGER NOT NULL,
    prev_hash     TEXT NOT NULL DEFAULT '',
    giver         TEXT NOT NULL DEFAULT '',
    receiver      TEXT NOT NULL DEFAULT '',
    nonce         TEXT NOT NULL DEFAULT '',
    event_hash    TEXT NOT NULL,
    canonical     TEXT NOT NULL,
    created_at    TEXT NOT NULL,
    UNIQUE(chain_id, seq),
    UNIQUE(chain_id, nonce),
    FOREIGN KEY(chain_id) REFERENCES chains(chain_id)
);

CREATE INDEX IF NOT EXISTS idx_events_chain_seq
    ON events(chain_id, seq);

CREATE TABLE IF NOT EXISTS offers (
{_OFFERS_COLUMNS}
);

CREATE INDEX IF NOT EXISTS idx_offers_chain
    ON offers(chain_id);

CREATE TABLE IF NOT EXISTS offer_witnesses (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    offer_id        TEXT NOT NULL,
    witness_index   INTEGER NOT NULL,
    name            TEXT NOT NULL,
    token_digest    TEXT NOT NULL,
    decision        TEXT,
    decided_at      TEXT,
    CHECK(decision IS NULL OR decision IN ('approved', 'rejected')),
    UNIQUE(offer_id, witness_index),
    UNIQUE(offer_id, token_digest),
    FOREIGN KEY(offer_id) REFERENCES offers(offer_id)
);

CREATE INDEX IF NOT EXISTS idx_offer_witnesses_offer
    ON offer_witnesses(offer_id, witness_index);
"""

# offers 表全部列（旧表重建搬运用，列序与 _OFFERS_COLUMNS 一致）
_OFFERS_ALL_COLUMNS = (
    "offer_id", "chain_id", "tail_hash", "receiver", "code_digest",
    "witness_threshold", "status", "expires_at", "created_at",
    "accepted_at", "accepted_seq", "cancel_code_digest", "cancelled_at",
    "replaced_at", "replaced_by",
)

# 最新结构必须具备的列：任一缺失即判定为旧表，触发就地重建
# （status 的 CHECK 约束无法直接探测，随重建一并更新为含 replaced）
_OFFERS_REQUIRED_COLUMNS = frozenset(
    {"cancel_code_digest", "cancelled_at", "replaced_at", "replaced_by"}
)


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(
        config.DB_PATH,
        timeout=10.0,
        isolation_level=None,  # 手动事务
        check_same_thread=False,
    )
    conn.row_factory = sqlite3.Row
    conn.execute(f"PRAGMA busy_timeout = 10000")
    conn.execute("PRAGMA journal_mode = WAL")
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


@contextmanager
def _get_conn() -> Iterator[sqlite3.Connection]:
    conn = _connect()
    try:
        yield conn
    finally:
        conn.close()


def _migrate_offers_if_needed(conn: sqlite3.Connection) -> None:
    """把旧版 offers 表就地重建为最新结构，存量行原样保留。

    SQLite 无法直接 ALTER 修改 CHECK 约束 / 追加 NOT NULL 列，故采用官方
    推荐的「建新表 -> 搬数据 -> 删旧表 -> 改名」流程，全程关闭外键约束。
    旧表缺失的列一律补 NULL：
    - witness_threshold：更早期版本无见证模式，NULL 表示无见证邀约；
    - cancel_code_digest / cancelled_at：旧邀约没有撤销码，不可撤销，
      行为与升级前完全一致；
    - replaced_at / replaced_by：旧邀约无替换关系（NULL），status 的
      CHECK 同步扩展为含 replaced。
    全新数据库（offers 已按 _SCHEMA 建出）无需重建。
    """
    offer_cols = {
        row["name"]
        for row in conn.execute("PRAGMA table_info(offers)").fetchall()
    }
    if not offer_cols:
        return
    if _OFFERS_REQUIRED_COLUMNS <= offer_cols:
        return

    # SELECT 表达式与 INSERT 列清单一一对应；旧表缺失的列以 NULL 补齐
    select_exprs = ", ".join(
        col if col in offer_cols else "NULL" for col in _OFFERS_ALL_COLUMNS
    )
    # 关闭/开启外键须在事务外；按 SQLite 官方「重建表」流程显式包一层
    # BEGIN ... COMMIT。新表先以临时名创建、删旧表后再改名：
    # offer_witnesses 与 replaced_by 的外键始终指向表名 offers，改名后
    # 引用自然恢复（replaced_by 成为新表的自引用外键）。
    conn.executescript(
        f"""
        PRAGMA foreign_keys = OFF;
        BEGIN;
        CREATE TABLE offers__new (
{_OFFERS_COLUMNS}
        );
        INSERT INTO offers__new
            ({", ".join(_OFFERS_ALL_COLUMNS)})
        SELECT {select_exprs}
        FROM offers;
        DROP TABLE offers;
        ALTER TABLE offers__new RENAME TO offers;
        CREATE INDEX IF NOT EXISTS idx_offers_chain ON offers(chain_id);
        COMMIT;
        PRAGMA foreign_keys = ON;
        """
    )
    failures = conn.execute("PRAGMA foreign_key_check").fetchall()
    if failures:
        raise RuntimeError(
            f"offers 表迁移后外键校验失败: {[tuple(r) for r in failures]}"
        )


def init_db() -> None:
    """初始化（或校验）数据库结构。可重复执行。

    对更早版本创建的 offers 表做一次性就地重建：增补 witness_threshold
    （无见证旧邀约为 NULL）、cancel_code_digest / cancelled_at
    （旧邀约撤销码为 NULL，即不可撤销）与 replaced_at / replaced_by
    （旧邀约无替换关系，为 NULL），并把 status 的 CHECK 扩展为
    pending / accepted / cancelled / replaced；存量邀约数据原样搬运，
    行为兼容。
    """
    config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    with _get_conn() as conn:
        conn.executescript(_SCHEMA)
        _migrate_offers_if_needed(conn)


@contextmanager
def write_tx() -> Iterator[sqlite3.Connection]:
    """串行化的写事务：BEGIN IMMEDIATE 立即取得写锁，提交前不会被插队。"""
    with _write_lock:
        conn = _connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()


@contextmanager
def read_tx() -> Iterator[sqlite3.Connection]:
    with _get_conn() as conn:
        conn.execute("BEGIN")
        try:
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
