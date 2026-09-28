"""测试共享夹具：每个用例独立内存库、确定性虚拟时钟，并打印判定日志。"""
from __future__ import annotations

import logging
import sys

import pytest

from app.service import EventService


class FakeClock:
    """从固定的“当前时间”开始，仅在显式 advance 后前进。"""

    def __init__(self, start_ms: int = 30_000) -> None:
        self.now = start_ms

    def __call__(self) -> int:
        return self.now

    def advance_to(self, ms: int) -> None:
        self.now = ms


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture
def service(clock: FakeClock) -> EventService:
    svc = EventService(":memory:", clock=clock)
    yield svc
    svc.close()


@pytest.fixture(scope="session", autouse=True)
def _enable_logs() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("[%(levelname)s] %(name)s: %(message)s"))
    root = logging.getLogger("event_aggregator")
    root.addHandler(handler)
    root.setLevel(logging.INFO)
    root.propagate = False
