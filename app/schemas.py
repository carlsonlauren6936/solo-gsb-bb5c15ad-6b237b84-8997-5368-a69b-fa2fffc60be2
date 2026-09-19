"""请求 / 响应模型与输入校验。"""
from __future__ import annotations

import re
from typing import Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

_HEX64 = re.compile(r"^[0-9a-f]{64}$")
_NAME_MAX = 200
_NONCE_MAX = 256
_CODE_MAX = 128
_TOKEN_MAX = 256
_OFFER_TTL_MAX = 7 * 24 * 3600  # 邀约有效期上限：7 天
_WITNESS_MIN = 2
_WITNESS_MAX = 5

def _validate_name(value: str, field: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field} 不能为空")
    if len(value) > _NAME_MAX:
        raise ValueError(f"{field} 长度不能超过 {_NAME_MAX}")
    return value


def _validate_hex64(value: str, field: str) -> str:
    value = value.strip().lower()
    if not _HEX64.match(value):
        raise ValueError(f"{field} 必须是 64 位十六进制哈希")
    return value


def _validate_code(value: str, field: str = "confirmation_code") -> str:
    value = value.strip()
    if not value:
        raise ValueError(f"{field} 不能为空")
    if len(value) > _CODE_MAX:
        raise ValueError(f"{field} 长度不能超过 {_CODE_MAX}")
    return value


def _validate_token(value: str) -> str:
    value = value.strip()
    if not value:
        raise ValueError("token 不能为空（见证人一次性令牌）")
    if len(value) > _TOKEN_MAX:
        raise ValueError(f"token 长度不能超过 {_TOKEN_MAX}")
    return value


class CreateChainRequest(BaseModel):
    file_hash: str = Field(..., description="交接文件的 SHA-256 摘要（64 位十六进制）")
    first_holder: str = Field(..., description="首位持有人标识")

    @field_validator("file_hash")
    @classmethod
    def _check_file_hash(cls, v: str) -> str:
        return _validate_hex64(v, "file_hash")

    @field_validator("first_holder")
    @classmethod
    def _check_first_holder(cls, v: str) -> str:
        return _validate_name(v, "first_holder")


class HandoverRequest(BaseModel):
    chain_id: str = Field(..., description="交接链 ID")
    tail_hash: str = Field(..., description="客户端认定的当前链尾哈希")
    receiver: str = Field(..., description="接收方标识")
    nonce: str = Field(..., description="一次性随机数，同一链内不可重复（防重放）")

    @field_validator("chain_id")
    @classmethod
    def _check_chain_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("chain_id 不能为空")
        return v

    @field_validator("tail_hash")
    @classmethod
    def _check_tail_hash(cls, v: str) -> str:
        return _validate_hex64(v, "tail_hash")

    @field_validator("receiver")
    @classmethod
    def _check_receiver(cls, v: str) -> str:
        return _validate_name(v, "receiver")

    @field_validator("nonce")
    @classmethod
    def _check_nonce(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("nonce 不能为空（一次性随机数）")
        if len(v) > _NONCE_MAX:
            raise ValueError(f"nonce 长度不能超过 {_NONCE_MAX}")
        return v


class EventOut(BaseModel):
    seq: int
    prev_hash: str
    giver: str
    receiver: str
    nonce: str
    event_hash: str
    canonical: str = Field(..., description="计算该哈希所用的紧凑 JSON 原文，便于审计复核")
    created_at: str


class ChainOut(BaseModel):
    chain_id: str
    file_hash: str
    first_holder: str
    tail_hash: str
    current_holder: str
    length: int = Field(..., description="事件总数（含创世记录，因此首次创建后为 1）")
    created_at: str
    events: list[EventOut]


class ChainSummary(BaseModel):
    chain_id: str
    file_hash: str
    current_holder: str
    tail_hash: str
    length: int
    created_at: str


class HandoverResult(BaseModel):
    chain_id: str
    seq: int
    prev_hash: str
    giver: str
    receiver: str
    nonce: str
    event_hash: str
    canonical: str
    tail_hash: str
    current_holder: str


class VerifyResult(BaseModel):
    chain_id: str
    valid: bool
    length: int
    expected_next_seq: int
    first_broken_seq: Optional[int] = Field(
        None, description="首个断裂事件的序号；链条头部信息被篡改时为 null"
    )
    reason: Optional[str] = None
    recomputed_tail_hash: Optional[str] = None
    stored_tail_hash: Optional[str] = None


class WitnessInput(BaseModel):
    name: str = Field(..., description="见证人标识")
    token: str = Field(
        ...,
        description=(
            "该见证人的独立一次性令牌（表态凭证，明文仅在创建响应中返回一次，"
            "服务端只保存其 SHA-256 摘要）"
        ),
    )

    @field_validator("name")
    @classmethod
    def _check_name(cls, v: str) -> str:
        return _validate_name(v, "witness name")

    @field_validator("token")
    @classmethod
    def _check_token(cls, v: str) -> str:
        return _validate_token(v)


class OfferCreateRequest(BaseModel):
    chain_id: str = Field(..., description="交接链 ID")
    tail_hash: str = Field(..., description="发起邀约时锁定的当前链尾哈希")
    receiver: str = Field(..., description="指定接收方标识（仅其应获知确认码）")
    expires_in_seconds: int = Field(
        ..., description="邀约有效期（秒），自创建时刻起算", ge=1, le=_OFFER_TTL_MAX
    )
    confirmation_code: str = Field(
        ..., description="一次性确认码，服务端只保存其 SHA-256 摘要"
    )
    cancellation_code: Optional[str] = Field(
        default=None,
        description=(
            "可选一次性撤销码：提供后邀约可凭此码调用取消接口撤销，"
            "服务端只保存「绑定 offer_id 做域分隔」的 SHA-256 摘要；"
            "省略则创建不可撤销的兼容旧行为邀约"
        ),
    )
    witnesses: Optional[list[WitnessInput]] = Field(
        default=None,
        description=(
            "见证人列表（2～5 名，各持独立一次性令牌）；"
            "省略或为空则创建兼容旧行为的无见证邀约"
        ),
    )
    witness_threshold: Optional[int] = Field(
        default=None,
        ge=1,
        description=(
            "见证通过阈值：接收方确认时同意数须达到该值且无拒绝；"
            "提供见证人时必填，合法范围 1～见证人数量（上限 5）"
        ),
    )

    @field_validator("chain_id")
    @classmethod
    def _check_chain_id(cls, v: str) -> str:
        v = v.strip()
        if not v:
            raise ValueError("chain_id 不能为空")
        return v

    @field_validator("tail_hash")
    @classmethod
    def _check_tail_hash(cls, v: str) -> str:
        return _validate_hex64(v, "tail_hash")

    @field_validator("receiver")
    @classmethod
    def _check_receiver(cls, v: str) -> str:
        return _validate_name(v, "receiver")

    @field_validator("confirmation_code")
    @classmethod
    def _check_confirmation_code(cls, v: str) -> str:
        return _validate_code(v)

    @field_validator("cancellation_code")
    @classmethod
    def _check_cancellation_code(cls, v: Optional[str]) -> Optional[str]:
        if v is None:
            return None
        code = _validate_code(v, "cancellation_code")
        return code or None

    @model_validator(mode="after")
    def _check_witnesses(self) -> "OfferCreateRequest":
        witnesses = self.witnesses or []
        if not witnesses:
            # 无见证邀约：阈值必须缺省
            if self.witness_threshold is not None:
                raise ValueError("无见证人时不能设置 witness_threshold")
            return self

        if not (_WITNESS_MIN <= len(witnesses) <= _WITNESS_MAX):
            raise ValueError(
                f"见证人数量必须在 {_WITNESS_MIN}～{_WITNESS_MAX} 名之间"
            )
        if self.witness_threshold is None:
            raise ValueError("见证邀约必须提供 witness_threshold")
        if not (1 <= self.witness_threshold <= len(witnesses)):
            raise ValueError("witness_threshold 必须在 1～见证人数量之间")

        names = [w.name for w in witnesses]
        if len(set(names)) != len(names):
            raise ValueError("见证人名称不能重复")
        tokens = [w.token for w in witnesses]
        if len(set(tokens)) != len(tokens):
            raise ValueError("见证人令牌不能重复")
        return self


class WitnessCreatedOut(BaseModel):
    witness_index: int = Field(..., description="见证人次序号（0 起，按提交顺序）")
    name: str
    token: str = Field(
        ...,
        description="该见证人的一次性令牌明文（仅此一次返回，服务端不存明文）",
    )


class OfferOut(BaseModel):
    offer_id: str
    chain_id: str
    receiver: str
    status: str = Field(
        ...,
        description=(
            "邀约状态：pending / accepted / expired / rejected / "
            "terminated / cancelled（后五者为终止或派生状态）"
        ),
    )
    expires_at: str
    created_at: str
    cancellable: bool = Field(
        default=False,
        description=(
            "是否可凭撤销码取消：仅创建时提供了 cancellation_code 的邀约为 true，"
            "旧邀约恒为 false"
        ),
    )
    witness_mode: bool = Field(
        default=False,
        description="是否见证确认模式（无见证邀约恒为 false）",
    )
    witness_threshold: Optional[int] = Field(
        default=None, description="见证通过阈值；无见证邀约为 null"
    )
    witnesses: list[WitnessCreatedOut] = Field(
        default_factory=list,
        description="见证人及其一次性令牌明文（仅创建响应返回）",
    )


class OfferConfirmRequest(BaseModel):
    confirmation_code: str = Field(..., description="发起邀约时约定的一次性确认码")

    @field_validator("confirmation_code")
    @classmethod
    def _check_confirmation_code(cls, v: str) -> str:
        return _validate_code(v)


class OfferCancelRequest(BaseModel):
    cancellation_code: str = Field(
        ..., description="发起邀约时可选提供的一次性撤销码（服务端只存其摘要）"
    )

    @field_validator("cancellation_code")
    @classmethod
    def _check_cancellation_code(cls, v: str) -> str:
        return _validate_code(v, "cancellation_code")


class OfferCancelResult(BaseModel):
    offer_id: str
    chain_id: str
    status: Literal["cancelled"] = Field(
        "cancelled", description="撤销成功后邀约状态固定为 cancelled"
    )
    cancellable: bool = Field(
        ..., description="固定为 true；不可撤销的邀约返回 409 而非本响应"
    )
    cancelled_at: str = Field(..., description="撤销时刻（UTC ISO-8601）")
    tail_hash: str = Field(
        ...,
        description=(
            "邀约锁定的链尾哈希；撤销不追加任何事件，它仍是当前链尾"
        ),
    )


class WitnessStateOut(BaseModel):
    witness_index: int
    name: str
    decision: Optional[Literal["approved", "rejected"]] = Field(
        None, description="表态结果：approved（同意）/ rejected（拒绝）/ null（未表态）"
    )
    decided_at: Optional[str] = None


class OfferStatusOut(BaseModel):
    offer_id: str
    chain_id: str
    receiver: str
    status: str = Field(
        ...,
        description=(
            "pending（待确认）/ accepted（已确认）/ expired（已过期，派生）/ "
            "rejected（见证模式下有见证人拒绝，派生）/ "
            "terminated（链尾已推进，派生）/ cancelled（已撤销，存储状态）"
        ),
    )
    expires_at: str
    created_at: str
    accepted_at: Optional[str] = None
    accepted_seq: Optional[int] = Field(
        None, description="确认后追加的移交事件序号；未确认时为 null"
    )
    cancellable: bool = Field(
        default=False,
        description=(
            "是否可凭撤销码取消；旧邀约（无撤销码）恒为 false，"
            "响应不含撤销码及其摘要"
        ),
    )
    cancelled_at: Optional[str] = Field(None, description="撤销时刻；未撤销为 null")
    witness_mode: bool = False
    witness_threshold: Optional[int] = Field(
        None, description="见证通过阈值；无见证邀约为 null"
    )
    witness_count: int = Field(
        0, description="见证人总数（无见证邀约为 0）"
    )
    approved_count: int = Field(0, description="已同意的见证人数")
    rejected_count: int = Field(0, description="已拒绝的见证人数")
    pending_count: int = Field(0, description="尚未表态的见证人数")
    witnesses: list[WitnessStateOut] = Field(
        default_factory=list,
        description="各见证人的表态状态，不含令牌及其摘要",
    )


class WitnessStatusOut(BaseModel):
    offer_id: str
    witness_mode: bool = Field(
        ..., description="固定为 true；对无见证邀约查询返回 404"
    )
    status: str = Field(
        ...,
        description=(
            "邀约派生状态：pending / accepted / expired / rejected / "
            "terminated / cancelled"
        ),
    )
    witness_threshold: int = Field(..., description="通过所需同意数")
    witness_count: int
    approved_count: int
    rejected_count: int
    pending_count: int
    witnesses: list[WitnessStateOut] = Field(
        ..., description="各见证人表态状态，绝不含令牌摘要"
    )


class WitnessDecisionRequest(BaseModel):
    token: str = Field(
        ..., description="创建邀约时由服务端分发的该见证人独立一次性令牌"
    )
    decision: Literal["approved", "rejected"] = Field(
        ..., description="表态：approved（同意）或 rejected（拒绝）"
    )

    @field_validator("token")
    @classmethod
    def _check_token(cls, v: str) -> str:
        return _validate_token(v)


class WitnessDecisionResult(BaseModel):
    offer_id: str
    witness_index: int
    name: str
    decision: Literal["approved", "rejected"]
    decided_at: str
    status: str = Field(
        ...,
        description=(
            "表态后邀约派生状态：pending（仍可继续表态/确认）/ rejected（已有拒绝）"
        ),
    )
    witness_threshold: int
    approved_count: int
    rejected_count: int
    pending_count: int


class OfferConfirmResult(BaseModel):
    offer_id: str
    chain_id: str
    seq: int
    prev_hash: str
    giver: str
    receiver: str
    nonce: str = Field(..., description="由邀约派生的一次性随机数（offer:<offer_id>）")
    event_hash: str
    canonical: str
    tail_hash: str
    current_holder: str


class HealthOut(BaseModel):
    status: str
