"""事件结构校验与兼容演进。

演进规则（保证旧/新数据混存下聚合结论可复现）：
- 每个事件类型有一个整数 schema_version；新版本只能“新增带默认值的可选字段”。
- 旧生产者缺省新字段时，按注册默认值补齐，聚合口径不变（如 amount 默认 0）。
- 必填字段缺失 -> MISSING_FIELD；值类型不符 -> TYPE_MISMATCH；
  未注册的事件类型 -> UNKNOWN_EVENT_TYPE。均显式拒绝，绝不静默丢弃。
- 未声明的额外字段予以保留（前向兼容），不影响已知字段的归一化结果。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Callable

from app.errors import RejectError, RejectReason


@dataclass(frozen=True)
class FieldSpec:
    name: str
    kind: str  # "str" | "int" | "number" | "bool"
    required: bool
    default: Any = None


@dataclass(frozen=True)
class EventSchema:
    name: str
    version: int
    fields: tuple[FieldSpec, ...]


def _is_str(v: Any) -> bool:
    return isinstance(v, str)


def _is_int(v: Any) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _is_number(v: Any) -> bool:
    return (isinstance(v, (int, float))) and not isinstance(v, bool)


def _is_bool(v: Any) -> bool:
    return isinstance(v, bool)


_CHECKERS: dict[str, Callable[[Any], bool]] = {
    "str": _is_str,
    "int": _is_int,
    "number": _is_number,
    "bool": _is_bool,
}


# 事件类型注册表。当前内置两个版本以演示“新增可选字段”的兼容演进：
# - purchase v1 只有必填 amount；v2 新增可选 coupon（默认 ""）。
# - click    v1 只有必填 target；v2 新增可选 amount（默认 0.0），参与 amount_sum。
EVENT_TYPES: dict[str, EventSchema] = {
    "purchase": EventSchema(
        name="purchase",
        version=2,
        fields=(
            FieldSpec("amount", "number", required=True),
            FieldSpec("coupon", "str", required=False, default=""),
        ),
    ),
    "click": EventSchema(
        name="click",
        version=1,
        fields=(
            FieldSpec("target", "str", required=True),
            FieldSpec("amount", "number", required=False, default=0.0),
        ),
    ),
}


def register_event_type(schema: EventSchema) -> None:
    """注册/升级事件类型（供测试与后续扩展使用）。"""
    EVENT_TYPES[schema.name] = schema


@dataclass(frozen=True)
class NormalizedEvent:
    event_type: str
    schema_version: int
    values: dict[str, Any]  # 已声明字段（缺省补齐默认值）
    extras: dict[str, Any]  # 未声明的额外字段（原样保留）


def validate_event(event_type: str, payload: dict[str, Any]) -> NormalizedEvent:
    schema = EVENT_TYPES.get(event_type)
    if schema is None:
        raise RejectError(
            RejectReason.UNKNOWN_EVENT_TYPE,
            f"未注册的事件类型: {event_type!r}",
        )

    values: dict[str, Any] = {}
    for spec in schema.fields:
        if spec.name not in payload or payload[spec.name] is None:
            if spec.required:
                raise RejectError(
                    RejectReason.MISSING_FIELD,
                    f"事件类型 {event_type!r} 缺少必填字段 {spec.name!r}",
                )
            values[spec.name] = spec.default
            continue
        value = payload[spec.name]
        if not _CHECKERS[spec.kind](value):
            raise RejectError(
                RejectReason.TYPE_MISMATCH,
                f"字段 {spec.name!r} 期望 {spec.kind}，实际为 "
                f"{type(value).__name__}: {value!r}",
            )
        values[spec.name] = value

    known = {spec.name for spec in schema.fields}
    extras = {k: v for k, v in payload.items() if k not in known}
    return NormalizedEvent(
        event_type=event_type,
        schema_version=schema.version,
        values=values,
        extras=extras,
    )
