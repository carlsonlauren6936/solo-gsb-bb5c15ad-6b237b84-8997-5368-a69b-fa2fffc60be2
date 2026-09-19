"""交接链核心业务逻辑。

哈希规则
--------
创世事件（seq=0）：
    canonical = sort_keys_json({"file_hash": ..., "holder": ..., "seq": 0})
    event_hash = SHA256(canonical)
移交事件（seq>=1）：
    canonical = sort_keys_json({
        "giver": 交出方, "nonce": 一次性随机数, "prev_hash": 前序哈希,
        "receiver": 接收方, "seq": 序号,
    })
    event_hash = SHA256(canonical)

交出方由服务端依据链尾持有人（current_holder）确定，客户端不能伪造。

待接收移交（邀约）流程
----------------------
1. 持有人以最新 tail_hash 发起邀约（指定 receiver、有效期、一次性确认码，
   可选一次性撤销码），服务端只保存确认码/撤销码摘要并返回 offer_id；
2. 接收方凭 offer_id + 确认码确认：在同一 SQLite 立即事务中校验邀约
   未过期、未使用且链尾仍匹配，然后追加唯一移交事件并将邀约置为 accepted；
3. 发起方也可凭 offer_id + 撤销码撤销（POST /offers/{offer_id}/cancel）：
   在同一 SQLite 立即事务中校验邀约仍 pending、未过期、无拒绝、链尾未推进，
   然后将邀约置为 cancelled，不追加任何链上事件。撤销与接收方确认、见证人
   表态互斥，竞争中最多只有一个终态成功；
4. 过期、重复确认、链尾已推进或已撤销均返回 409 且不写事件。

撤销码规则
----------
- 创建邀约时 cancellation_code 可选：省略则邀约不可撤销（与旧邀约一致）；
- 服务端只保存「offer:<offer_id>:cancel-code:<code>」的 SHA-256 摘要，
  摘要域绑定 offer_id 且与确认码、见证令牌分属不同域，明文绝不落库；
- 凭码撤销：撤销码错误返回 403 cancel_code_mismatch；邀约已 accepted、
  rejected（派生）、expired（派生）、terminated（派生）、cancelled 或
  根本没有撤销码时返回 409 offer_not_cancellable，且不改写哈希链。

见证确认模式（可选）
--------------------
发起邀约时可提交 2～5 名见证人及通过阈值 threshold：
1. 每位见证人持有一个互相独立的一次性令牌；服务端只保存
   「offer:<offer_id>:witness-token:<token>」的 SHA-256 摘要，明文不落库；
2. 见证人凭令牌调用表态接口同意（approved）或拒绝（rejected），
   令牌命中即作废——同一见证人至多表态一次，重复使用令牌返回 409；
3. 任一拒绝都会使邀约进入 rejected 终止态，后续确认/表态一律 409，
   且不追加任何链上事件；
4. 接收方确认时，在同一个 SQLite 立即事务内依次校验：确认码摘要 →
   邀约未过期 → 邀约未使用 → 链尾仍匹配 → 无拒绝且同意数达到阈值，
   全部通过才追加移交事件。拒绝（offer_rejected）、见证数不足
   （witness_threshold_not_met）、过期、链尾推进均为 409 且不写事件；
5. 无见证邀约（不提交见证人）行为与上述普通邀约完全一致，向后兼容。
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from .database import read_tx, write_tx
from .hashing import canonical_json, digest_hex


class ChainError(Exception):
    """业务错误：携带 HTTP 状态码。"""

    def __init__(self, status_code: int, code: str, message: str):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.message = message


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _now_dt() -> datetime:
    return datetime.now(timezone.utc)


def _parse_ts(value: str) -> datetime:
    return datetime.fromisoformat(value)


def _code_digest(code: str) -> str:
    """一次性确认码的存储摘要：带域分隔的 SHA-256，绝不落库明文。"""
    return digest_hex(f"offer-code:{code}")


def _witness_token_digest(offer_id: str, token: str) -> str:
    """见证人一次性令牌的存储摘要。

    摘要域中混入 offer_id：同一令牌字符串在不同邀约间不可复用，
    且与确认码摘要分属不同域。绝不落库明文。
    """
    return digest_hex(f"offer:{offer_id}:witness-token:{token}")


def _cancel_code_digest(offer_id: str, code: str) -> str:
    """一次性撤销码的存储摘要。

    摘要域绑定 offer_id（同一撤销码字符串在不同邀约间不可复用），且与
    确认码摘要（offer-code:）、见证令牌摘要（:witness-token:）分属不同域。
    绝不落库明文。
    """
    return digest_hex(f"offer:{offer_id}:cancel-code:{code}")


def _genesis_payload(file_hash: str, holder: str) -> dict[str, Any]:
    return {"file_hash": file_hash, "holder": holder, "seq": 0}


def _handover_payload(
    seq: int, prev_hash: str, giver: str, receiver: str, nonce: str
) -> dict[str, Any]:
    # 键名顺序无所谓 —— canonical_json 会 sort_keys；显式写出仅为可读性。
    return {
        "giver": giver,
        "nonce": nonce,
        "prev_hash": prev_hash,
        "receiver": receiver,
        "seq": seq,
    }


def _row_to_event(row: sqlite3.Row) -> dict[str, Any]:
    return {
        "seq": row["seq"],
        "prev_hash": row["prev_hash"],
        "giver": row["giver"],
        "receiver": row["receiver"],
        "nonce": row["nonce"],
        "event_hash": row["event_hash"],
        "canonical": row["canonical"],
        "created_at": row["created_at"],
    }


def create_chain(file_hash: str, first_holder: str) -> dict[str, Any]:
    """创建交接链：写入创世记录（序号 0）。原子完成。"""
    chain_id = uuid.uuid4().hex
    created_at = _now()

    payload = _genesis_payload(file_hash, first_holder)
    canonical = canonical_json(payload)
    genesis_hash = digest_hex(canonical)

    try:
        with write_tx() as conn:
            conn.execute(
                """INSERT INTO chains
                   (chain_id, file_hash, first_holder, tail_hash,
                    current_holder, created_at)
                   VALUES (?, ?, ?, ?, ?, ?)""",
                (chain_id, file_hash, first_holder, genesis_hash,
                 first_holder, created_at),
            )
            conn.execute(
                """INSERT INTO events
                   (chain_id, seq, prev_hash, giver, receiver, nonce,
                    event_hash, canonical, created_at)
                   VALUES (?, 0, '', '', '', '', ?, ?, ?)""",
                (chain_id, genesis_hash, canonical, created_at),
            )
    except sqlite3.IntegrityError as exc:  # 理论上 UUID 冲突才会走到这里
        raise ChainError(409, "conflict", f"链条创建冲突: {exc}") from exc

    return {
        "chain_id": chain_id,
        "file_hash": file_hash,
        "first_holder": first_holder,
        "tail_hash": genesis_hash,
        "current_holder": first_holder,
        "length": 1,
        "created_at": created_at,
        "events": [
            {
                "seq": 0,
                "prev_hash": "",
                "giver": "",
                "receiver": "",
                "nonce": "",
                "event_hash": genesis_hash,
                "canonical": canonical,
                "created_at": created_at,
            }
        ],
    }


def _append_handover_tx(
    conn: sqlite3.Connection, chain: sqlite3.Row, receiver: str, nonce: str
) -> dict[str, Any]:
    """在已开启的写事务内向链尾追加一次移交事件并推进链头。

    调用方必须已完成链尾匹配等前置校验；本函数负责算哈希、插事件、
    更新链头，与调用方的校验同处一个事务，整体原子提交或回滚。
    """
    chain_id = chain["chain_id"]
    giver = chain["current_holder"]
    last_event = conn.execute(
        "SELECT seq FROM events WHERE chain_id = ? ORDER BY seq DESC LIMIT 1",
        (chain_id,),
    ).fetchone()
    seq = last_event["seq"] + 1

    payload = _handover_payload(
        seq=seq,
        prev_hash=chain["tail_hash"],
        giver=giver,
        receiver=receiver,
        nonce=nonce,
    )
    canonical = canonical_json(payload)
    new_hash = digest_hex(canonical)
    created_at = _now()

    try:
        conn.execute(
            """INSERT INTO events
               (chain_id, seq, prev_hash, giver, receiver, nonce,
                event_hash, canonical, created_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (chain_id, seq, chain["tail_hash"], giver, receiver, nonce,
             new_hash, canonical, created_at),
        )
    except sqlite3.IntegrityError as exc:
        msg = str(exc)
        if "nonce" in msg:
            raise ChainError(
                409, "nonce_replay", "随机数重放被拒绝（并发请求）"
            ) from exc
        raise ChainError(409, "conflict", f"追加冲突: {msg}") from exc

    conn.execute(
        """UPDATE chains
           SET tail_hash = ?, current_holder = ?
           WHERE chain_id = ?""",
        (new_hash, receiver, chain_id),
    )

    return {
        "chain_id": chain_id,
        "seq": seq,
        "prev_hash": chain["tail_hash"],
        "giver": giver,
        "receiver": receiver,
        "nonce": nonce,
        "event_hash": new_hash,
        "canonical": canonical,
        "tail_hash": new_hash,
        "current_holder": receiver,
    }


def append_handover(
    chain_id: str, tail_hash: str, receiver: str, nonce: str
) -> dict[str, Any]:
    """追加一次移交。

    冲突（409）情形：链条不存在 -> 404；链尾哈希不匹配（过期/竞争请求）；
    随机数在该链内重放。任何冲突都不会写入数据，链条不会分叉。
    """
    with write_tx() as conn:
        chain = conn.execute(
            "SELECT * FROM chains WHERE chain_id = ?", (chain_id,)
        ).fetchone()
        if chain is None:
            raise ChainError(404, "not_found", f"链条不存在: {chain_id}")

        if chain["tail_hash"] != tail_hash:
            raise ChainError(
                409,
                "tail_mismatch",
                "链尾哈希不匹配：链条已被其他移交推进，请获取最新链尾后重试",
            )

        # UNIQUE(chain_id, nonce) 是最终防线；这里先给出明确错误信息。
        dup = conn.execute(
            "SELECT seq FROM events WHERE chain_id = ? AND nonce = ? AND seq > 0",
            (chain_id, nonce),
        ).fetchone()
        if dup is not None:
            raise ChainError(
                409,
                "nonce_replay",
                f"随机数已被使用（重放被拒绝），首次出现于序号 {dup['seq']}",
            )

        result = _append_handover_tx(conn, chain, receiver, nonce)

    return result


def _load_witnesses(
    conn: sqlite3.Connection, offer_id: str
) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT * FROM offer_witnesses WHERE offer_id = ? ORDER BY witness_index",
        (offer_id,),
    ).fetchall()


def _witness_summary(
    witness_rows: list[sqlite3.Row],
) -> tuple[int, int, int, list[dict[str, Any]]]:
    """统计见证人表态，并构造对外可见的状态列表（不含令牌摘要）。"""
    approved = rejected = pending = 0
    states: list[dict[str, Any]] = []
    for row in witness_rows:
        decision = row["decision"]
        if decision == "approved":
            approved += 1
        elif decision == "rejected":
            rejected += 1
        else:
            pending += 1
        states.append(
            {
                "witness_index": row["witness_index"],
                "name": row["name"],
                "decision": decision,
                "decided_at": row["decided_at"],
            }
        )
    return approved, rejected, pending, states


def _derived_offer_status(
    offer: sqlite3.Row,
    witness_rows: list[sqlite3.Row],
    current_tail_hash: Optional[str],
    now: Optional[datetime] = None,
) -> str:
    """邀约对外状态。

    存储状态为 pending / accepted / cancelled；其余均为派生状态：
    - rejected：pending 且见证模式下已有见证人拒绝（一旦拒绝不可恢复）；
    - expired：pending 且当前时间已过有效期；
    - terminated：pending 且邀约锁定的链尾已被其他移交推进；
    派生顺序为 accepted > cancelled > rejected > expired > terminated >
    pending（accepted 与 cancelled 互斥，二者不会同时出现）。
    """
    if offer["status"] == "accepted":
        return "accepted"

    if offer["status"] == "cancelled":
        return "cancelled"

    for row in witness_rows:
        if row["decision"] == "rejected":
            return "rejected"

    now = now or _now_dt()
    if now > _parse_ts(offer["expires_at"]):
        return "expired"

    if current_tail_hash is not None and current_tail_hash != offer["tail_hash"]:
        return "terminated"

    return "pending"


def _offer_to_dict(
    offer: sqlite3.Row,
    witness_rows: list[sqlite3.Row],
    current_tail_hash: Optional[str] = None,
    now: Optional[datetime] = None,
) -> dict[str, Any]:
    """邀约行 + 见证行 -> 对外字典（任何情况下都不返回确认码/令牌摘要）。"""
    approved, rejected, pending, states = _witness_summary(witness_rows)
    witness_mode = offer["witness_threshold"] is not None
    return {
        "offer_id": offer["offer_id"],
        "chain_id": offer["chain_id"],
        "receiver": offer["receiver"],
        "status": _derived_offer_status(
            offer, witness_rows, current_tail_hash, now
        ),
        "expires_at": offer["expires_at"],
        "created_at": offer["created_at"],
        "accepted_at": offer["accepted_at"],
        "accepted_seq": offer["accepted_seq"],
        "cancellable": offer["cancel_code_digest"] is not None,
        "cancelled_at": offer["cancelled_at"],
        "witness_mode": witness_mode,
        "witness_threshold": offer["witness_threshold"],
        "witness_count": len(witness_rows),
        "approved_count": approved,
        "rejected_count": rejected,
        "pending_count": pending,
        "witnesses": states,
    }


def create_offer(
    chain_id: str,
    tail_hash: str,
    receiver: str,
    expires_in_seconds: int,
    confirmation_code: str,
    witnesses: Optional[list[dict[str, str]]] = None,
    witness_threshold: Optional[int] = None,
    cancellation_code: Optional[str] = None,
) -> dict[str, Any]:
    """发起待接收移交邀约。

    以当前链尾 tail_hash 锁定邀约：链尾不匹配（链条已推进）返回 409，
    邀约创建即无意义。确认码只保存 SHA-256 摘要，明文不落库。

    cancellation_code 非空时邀约「可撤销」：保存绑定 offer_id 做域分隔的
    SHA-256 摘要（offer:<offer_id>:cancel-code:<code>），此后可凭码调用
    cancel_offer；省略时 cancel_code_digest 为 NULL，邀约不可撤销，行为与
    旧版本完全一致。

    witnesses 非空时创建见证确认模式邀约（2～5 名见证人 + 通过阈值），
    每位见证人的独立一次性令牌同样只保存摘要；创建响应中一次性返回
    令牌明文，由发起方线下分发，此后服务端不再展示。
    """
    with write_tx() as conn:
        chain = conn.execute(
            "SELECT * FROM chains WHERE chain_id = ?", (chain_id,)
        ).fetchone()
        if chain is None:
            raise ChainError(404, "not_found", f"链条不存在: {chain_id}")

        if chain["tail_hash"] != tail_hash:
            raise ChainError(
                409,
                "tail_mismatch",
                "链尾哈希不匹配：链条已被其他移交推进，请获取最新链尾后再发起邀约",
            )

        offer_id = uuid.uuid4().hex
        now = _now_dt()
        created_at = now.isoformat(timespec="seconds")
        expires_at = (now + timedelta(seconds=expires_in_seconds)).isoformat(
            timespec="seconds"
        )

        cancel_digest = (
            _cancel_code_digest(offer_id, cancellation_code)
            if cancellation_code is not None
            else None
        )

        conn.execute(
            """INSERT INTO offers
               (offer_id, chain_id, tail_hash, receiver, code_digest,
                witness_threshold, status, expires_at, created_at,
                cancel_code_digest, cancelled_at)
               VALUES (?, ?, ?, ?, ?, ?, 'pending', ?, ?, ?, NULL)""",
            (
                offer_id,
                chain_id,
                tail_hash,
                receiver,
                _code_digest(confirmation_code),
                witness_threshold if witnesses else None,
                expires_at,
                created_at,
                cancel_digest,
            ),
        )

        created_witnesses: list[dict[str, Any]] = []
        if witnesses:
            for index, witness in enumerate(witnesses):
                conn.execute(
                    """INSERT INTO offer_witnesses
                       (offer_id, witness_index, name, token_digest,
                        decision, decided_at)
                       VALUES (?, ?, ?, ?, NULL, NULL)""",
                    (
                        offer_id,
                        index,
                        witness["name"],
                        _witness_token_digest(offer_id, witness["token"]),
                    ),
                )
                created_witnesses.append(
                    {
                        "witness_index": index,
                        "name": witness["name"],
                        "token": witness["token"],
                    }
                )

    return {
        "offer_id": offer_id,
        "chain_id": chain_id,
        "receiver": receiver,
        "status": "pending",
        "expires_at": expires_at,
        "created_at": created_at,
        "cancellable": cancellation_code is not None,
        "witness_mode": bool(witnesses),
        "witness_threshold": witness_threshold if witnesses else None,
        "witnesses": created_witnesses,
    }


def confirm_offer(offer_id: str, confirmation_code: str) -> dict[str, Any]:
    """接收方凭 offer_id + 确认码确认邀约，追加唯一移交事件。

    所有校验与写入在同一个 SQLite 立即事务（BEGIN IMMEDIATE）中完成：
    校验确认码摘要 -> 邀约未过期 -> 邀约未使用 -> 链尾仍匹配 ->
    （见证模式）无拒绝且同意数达到阈值，然后追加移交事件、推进链头
    并将邀约置为 accepted，整体原子提交。
    过期、重复确认、见证拒绝、同意数不足、链尾已推进均返回 409
    且整体回滚，不写任何事件。
    """
    with write_tx() as conn:
        offer = conn.execute(
            "SELECT * FROM offers WHERE offer_id = ?", (offer_id,)
        ).fetchone()
        if offer is None:
            raise ChainError(404, "not_found", f"邀约不存在: {offer_id}")

        if offer["status"] == "cancelled":
            raise ChainError(
                409, "offer_not_cancellable", "邀约已撤销，不能确认，也不再可撤销"
            )

        if offer["code_digest"] != _code_digest(confirmation_code):
            raise ChainError(403, "code_mismatch", "确认码不正确")

        if _now_dt() > _parse_ts(offer["expires_at"]):
            raise ChainError(
                409, "offer_expired", "邀约已过期，请由持有人重新发起邀约"
            )

        if offer["status"] != "pending":
            raise ChainError(
                409, "offer_already_accepted", "邀约已被确认，不能重复使用"
            )

        chain = conn.execute(
            "SELECT * FROM chains WHERE chain_id = ?", (offer["chain_id"],)
        ).fetchone()
        if chain is None:  # 外键约束下理论上不可达
            raise ChainError(404, "not_found", f"链条不存在: {offer['chain_id']}")

        if chain["tail_hash"] != offer["tail_hash"]:
            raise ChainError(
                409,
                "tail_mismatch",
                "链尾已推进：邀约所锁定的链尾不再是最新，邀约终止",
            )

        if offer["witness_threshold"] is not None:
            witness_rows = _load_witnesses(conn, offer_id)
            approved, rejected, _pending, _states = _witness_summary(witness_rows)
            if rejected > 0:
                raise ChainError(
                    409,
                    "offer_rejected",
                    f"已有 {rejected} 名见证人拒绝，邀约终止，不能确认",
                )
            if approved < offer["witness_threshold"]:
                raise ChainError(
                    409,
                    "witness_threshold_not_met",
                    "见证同意数未达到通过阈值："
                    f"{approved}/{offer['witness_threshold']}",
                )

        # nonce 由邀约派生，天然一次性：同一邀约不可能确认出第二个事件，
        # 也不会与手工移交的 nonce 冲突；UNIQUE(chain_id, nonce) 兜底。
        result = _append_handover_tx(
            conn, chain, offer["receiver"], nonce=f"offer:{offer_id}"
        )

        conn.execute(
            """UPDATE offers
               SET status = 'accepted', accepted_at = ?, accepted_seq = ?
               WHERE offer_id = ?""",
            (_now(), result["seq"], offer_id),
        )

    result["offer_id"] = offer_id
    return result


def cancel_offer(offer_id: str, cancellation_code: str) -> dict[str, Any]:
    """发起方凭 offer_id + 撤销码撤销邀约，不追加任何链上事件。

    所有校验与写入在同一个 SQLite 立即事务（BEGIN IMMEDIATE）中完成，与
    接收方确认、见证人表态互斥：进程内由写锁串行，进程间由 SQLite 写锁
    串行，竞争终态（accepted / cancelled / rejected …）中至多一个成功。

    可撤销的充要条件：存储状态仍为 pending、未过期、无见证人拒绝、锁定的
    链尾仍是当前链尾。任一不满足（含邀约已 accepted / rejected / expired /
    terminated / cancelled，以及创建时未提供撤销码的旧邀约）返回 409
    offer_not_cancellable 且不改写任何数据、不动哈希链；撤销码错误返回
    403 cancel_code_mismatch。撤销码摘要本身从不出现在任何响应中。
    """
    with write_tx() as conn:
        offer = conn.execute(
            "SELECT * FROM offers WHERE offer_id = ?", (offer_id,)
        ).fetchone()
        if offer is None:
            raise ChainError(404, "not_found", f"邀约不存在: {offer_id}")

        # 终态 / 无撤销码：一律 409 offer_not_cancellable，不区分具体原因，
        # 且在验码之前判定 —— 不具备撤销条件时不泄露撤销码是否匹配。
        if offer["status"] != "pending":
            if offer["status"] == "accepted":
                message = "邀约已被接收方确认，不能撤销"
            else:  # cancelled
                message = "邀约已撤销，不能重复撤销"
            raise ChainError(409, "offer_not_cancellable", message)

        if _now_dt() > _parse_ts(offer["expires_at"]):
            raise ChainError(
                409,
                "offer_not_cancellable",
                "邀约已过期，不能撤销（过期为终止态）",
            )

        chain = conn.execute(
            "SELECT tail_hash FROM chains WHERE chain_id = ?",
            (offer["chain_id"],),
        ).fetchone()
        if chain is None:  # 外键约束下理论上不可达
            raise ChainError(404, "not_found", f"链条不存在: {offer['chain_id']}")
        if chain["tail_hash"] != offer["tail_hash"]:
            raise ChainError(
                409,
                "offer_not_cancellable",
                "链尾已推进：邀约所锁定的链尾不再是最新，不能撤销",
            )

        witness_rows = _load_witnesses(conn, offer_id)
        if any(r["decision"] == "rejected" for r in witness_rows):
            raise ChainError(
                409,
                "offer_not_cancellable",
                "已有见证人拒绝，邀约已终止，不能撤销",
            )

        if offer["cancel_code_digest"] is None:
            # 旧邀约 / 创建时省略 cancellation_code：兼容行为，不可撤销。
            raise ChainError(
                409,
                "offer_not_cancellable",
                "该邀约未设置撤销码，不能撤销",
            )

        if offer["cancel_code_digest"] != _cancel_code_digest(
            offer_id, cancellation_code
        ):
            raise ChainError(403, "cancel_code_mismatch", "撤销码不正确")

        cancelled_at = _now()
        # WHERE 条件再兜底一次：本事务内竞态被写锁排除，影响行数不为 1
        # 只可能意味着状态已被其他终态占据，此时绝不写入。
        cursor = conn.execute(
            """UPDATE offers
               SET status = 'cancelled', cancelled_at = ?
               WHERE offer_id = ? AND status = 'pending'""",
            (cancelled_at, offer_id),
        )
        if cursor.rowcount != 1:
            raise ChainError(
                409,
                "offer_not_cancellable",
                "邀约已离开 pending 状态（竞争终止），撤销未生效",
            )

    return {
        "offer_id": offer_id,
        "chain_id": offer["chain_id"],
        "status": "cancelled",
        "cancellable": True,
        "cancelled_at": cancelled_at,
        "tail_hash": offer["tail_hash"],
    }


def record_witness_decision(
    offer_id: str, token: str, decision: str
) -> dict[str, Any]:
    """见证人凭独立一次性令牌表态同意 / 拒绝。

    令牌以摘要比对（摘要域绑定 offer_id），同一见证人至多表态一次：
    令牌一旦命中一行且该行已有 decision，即返回 409 witness_already_decided。
    所有校验与写入在同一个 SQLite 立即事务内完成。

    邀约本身终止（不存在 404；确认码错误不属于本流程；已接受、已过期、
    链尾已推进、已被其他见证人拒绝）时表态一律 409，且不会改动任何数据。
    返回的表态结果绝不包含令牌摘要。
    """
    with write_tx() as conn:
        offer = conn.execute(
            "SELECT * FROM offers WHERE offer_id = ?", (offer_id,)
        ).fetchone()
        if offer is None:
            raise ChainError(404, "not_found", f"邀约不存在: {offer_id}")

        if offer["witness_threshold"] is None:
            raise ChainError(
                409,
                "offer_not_witnessed",
                "该邀约不是见证确认模式，没有见证人",
            )

        witness = conn.execute(
            "SELECT * FROM offer_witnesses WHERE offer_id = ? AND token_digest = ?",
            (offer_id, _witness_token_digest(offer_id, token)),
        ).fetchone()
        if witness is None:
            # 与确认码错误一致：凭证无效返回 403，且不区分「不存在/已用」。
            raise ChainError(403, "token_mismatch", "见证人令牌不正确")

        if witness["decision"] is not None:
            raise ChainError(
                409,
                "witness_already_decided",
                f"见证人 {witness['name']} 已表态，令牌为一次性，不能重复使用",
            )

        if offer["status"] == "cancelled":
            raise ChainError(
                409,
                "offer_not_cancellable",
                "邀约已撤销，表态无效，也不再可撤销",
            )

        if offer["status"] != "pending":
            raise ChainError(
                409, "offer_already_accepted", "邀约已被确认，不能继续表态"
            )

        if _now_dt() > _parse_ts(offer["expires_at"]):
            raise ChainError(409, "offer_expired", "邀约已过期，表态无效")

        chain = conn.execute(
            "SELECT tail_hash FROM chains WHERE chain_id = ?",
            (offer["chain_id"],),
        ).fetchone()
        if chain is None:  # 外键约束下理论上不可达
            raise ChainError(404, "not_found", f"链条不存在: {offer['chain_id']}")
        if chain["tail_hash"] != offer["tail_hash"]:
            raise ChainError(
                409,
                "tail_mismatch",
                "链尾已推进：邀约所锁定的链尾不再是最新，邀约终止",
            )

        witness_rows = _load_witnesses(conn, offer_id)
        if any(r["decision"] == "rejected" for r in witness_rows):
            raise ChainError(
                409, "offer_rejected", "已有见证人拒绝，邀约终止，不能继续表态"
            )

        decided_at = _now()
        conn.execute(
            """UPDATE offer_witnesses
               SET decision = ?, decided_at = ?
               WHERE offer_id = ? AND witness_index = ?""",
            (decision, decided_at, offer_id, witness["witness_index"]),
        )

        witness_rows = _load_witnesses(conn, offer_id)
        approved, rejected, pending, _states = _witness_summary(witness_rows)

    return {
        "offer_id": offer_id,
        "witness_index": witness["witness_index"],
        "name": witness["name"],
        "decision": decision,
        "decided_at": decided_at,
        "status": "rejected" if rejected > 0 else "pending",
        "witness_threshold": offer["witness_threshold"],
        "approved_count": approved,
        "rejected_count": rejected,
        "pending_count": pending,
    }


def get_offer(offer_id: str) -> dict[str, Any]:
    """查询单个邀约状态（不返回确认码/令牌及其摘要）。"""
    with read_tx() as conn:
        offer = conn.execute(
            "SELECT * FROM offers WHERE offer_id = ?", (offer_id,)
        ).fetchone()
        if offer is None:
            raise ChainError(404, "not_found", f"邀约不存在: {offer_id}")
        witness_rows = _load_witnesses(conn, offer_id)
        chain = conn.execute(
            "SELECT tail_hash FROM chains WHERE chain_id = ?",
            (offer["chain_id"],),
        ).fetchone()

    current_tail = chain["tail_hash"] if chain is not None else None
    return _offer_to_dict(offer, witness_rows, current_tail)


def get_witness_status(offer_id: str) -> dict[str, Any]:
    """查询见证邀约的见证人表态状态；无见证邀约返回 404。

    响应只包含各见证人的名称、序号与表态结果，绝不包含令牌摘要。
    """
    with read_tx() as conn:
        offer = conn.execute(
            "SELECT * FROM offers WHERE offer_id = ?", (offer_id,)
        ).fetchone()
        if offer is None:
            raise ChainError(404, "not_found", f"邀约不存在: {offer_id}")
        if offer["witness_threshold"] is None:
            raise ChainError(
                404, "witness_mode_not_found", "该邀约不是见证确认模式"
            )
        witness_rows = _load_witnesses(conn, offer_id)
        chain = conn.execute(
            "SELECT tail_hash FROM chains WHERE chain_id = ?",
            (offer["chain_id"],),
        ).fetchone()

    current_tail = chain["tail_hash"] if chain is not None else None
    data = _offer_to_dict(offer, witness_rows, current_tail)
    return {
        "offer_id": offer_id,
        "witness_mode": True,
        "status": data["status"],
        "witness_threshold": offer["witness_threshold"],
        "witness_count": data["witness_count"],
        "approved_count": data["approved_count"],
        "rejected_count": data["rejected_count"],
        "pending_count": data["pending_count"],
        "witnesses": data["witnesses"],
    }


def get_chain(chain_id: str) -> dict[str, Any]:
    with read_tx() as conn:
        chain = conn.execute(
            "SELECT * FROM chains WHERE chain_id = ?", (chain_id,)
        ).fetchone()
        if chain is None:
            raise ChainError(404, "not_found", f"链条不存在: {chain_id}")

        rows = conn.execute(
            "SELECT * FROM events WHERE chain_id = ? ORDER BY seq", (chain_id,)
        ).fetchall()

    return {
        "chain_id": chain["chain_id"],
        "file_hash": chain["file_hash"],
        "first_holder": chain["first_holder"],
        "tail_hash": chain["tail_hash"],
        "current_holder": chain["current_holder"],
        "length": len(rows),
        "created_at": chain["created_at"],
        "events": [_row_to_event(r) for r in rows],
    }


def list_chains() -> list[dict[str, Any]]:
    with read_tx() as conn:
        rows = conn.execute(
            """SELECT c.*, COUNT(e.seq) AS length
               FROM chains c
               LEFT JOIN events e ON e.chain_id = c.chain_id
               GROUP BY c.chain_id
               ORDER BY c.created_at, c.chain_id"""
        ).fetchall()
    return [
        {
            "chain_id": r["chain_id"],
            "file_hash": r["file_hash"],
            "current_holder": r["current_holder"],
            "tail_hash": r["tail_hash"],
            "length": r["length"],
            "created_at": r["created_at"],
        }
        for r in rows
    ]


def verify_chain(chain_id: str) -> dict[str, Any]:
    """重放全链并指出首个断裂序号。

    对每个事件重新计算哈希并与存储值比对，同时校验序号连续与 prev_hash 指向。
    另将重算出的链尾与 chains 头信息比对，以发现头部被直接篡改的情况。
    """
    with read_tx() as conn:
        chain = conn.execute(
            "SELECT * FROM chains WHERE chain_id = ?", (chain_id,)
        ).fetchone()
        if chain is None:
            raise ChainError(404, "not_found", f"链条不存在: {chain_id}")
        rows = conn.execute(
            "SELECT * FROM events WHERE chain_id = ? ORDER BY seq", (chain_id,)
        ).fetchall()

    length = len(rows)
    expected_next_seq = length
    prev_hash = ""
    broken_seq: Optional[int] = None
    reason: Optional[str] = None

    for i, row in enumerate(rows):
        if row["seq"] != i:
            broken_seq, reason = row["seq"], f"序号不连续：期望 {i}，实际 {row['seq']}"
            break

        if row["seq"] == 0:
            payload = _genesis_payload(chain["file_hash"], chain["first_holder"])
        else:
            payload = _handover_payload(
                seq=row["seq"],
                prev_hash=row["prev_hash"],
                giver=row["giver"],
                receiver=row["receiver"],
                nonce=row["nonce"],
            )
        recomputed = digest_hex(canonical_json(payload))

        if recomputed != row["event_hash"]:
            broken_seq, reason = (
                row["seq"],
                f"事件哈希不匹配：记录 {row['event_hash']}，重算 {recomputed}"
                + ("（文件摘要或首位持有人被篡改）" if row["seq"] == 0 else ""),
            )
            break

        if row["event_hash"] != row["prev_hash"] and row["prev_hash"] != prev_hash:
            broken_seq, reason = (
                row["seq"],
                f"前序指针断裂：期望指向 {prev_hash}，实际指向 {row['prev_hash']}",
            )
            break

        prev_hash = row["event_hash"]

    recomputed_tail = prev_hash if broken_seq is None else None

    if broken_seq is None and recomputed_tail != chain["tail_hash"]:
        # 事件本身全部可验证，但链条头的链尾缓存被篡改：没有具体事件序号可指
        reason = (
            "链条头部信息被篡改：链尾哈希为 "
            f"{chain['tail_hash']}，按事件重算应为 {recomputed_tail}"
        )
        # first_broken_seq 保持 null
    valid = broken_seq is None and recomputed_tail == chain["tail_hash"]
    if valid and length > 0:
        # 最后一个事件的接收方必须等于头信息中的当前持有人
        last_holder = chain["first_holder"] if length == 1 else rows[-1]["receiver"]
        if chain["current_holder"] != last_holder:
            valid = False
            reason = (
                "链条头部信息被篡改：当前持有人为 "
                f"{chain['current_holder']}，按事件推导应为 {last_holder}"
            )

    return {
        "chain_id": chain_id,
        "valid": valid,
        "length": length,
        "expected_next_seq": expected_next_seq,
        "first_broken_seq": broken_seq,
        "reason": reason,
        "recomputed_tail_hash": recomputed_tail,
        "stored_tail_hash": chain["tail_hash"],
    }
