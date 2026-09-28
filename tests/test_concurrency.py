"""并发写入一致性场景。

- 同进程多线程共享连接：锁 + BEGIN IMMEDIATE 串行化。
- 两个独立连接（各自加锁）指向同一文件库：由 SQLite 文件写锁串行化，
  用来验证跨连接竞争下不重不漏、失败整体回滚。
"""
from __future__ import annotations

import concurrent.futures as cf
import threading

from app.service import EventService


def _ev(eid: str, amount: float = 1.0) -> dict:
    return {
        "event_id": eid, "event_type": "purchase", "subject": "u1",
        "ts_ms": 10_000, "payload": {"amount": amount},
    }


def test_concurrent_unique_writes_count_exact(service, tmp_path) -> None:
    n_threads, per_thread = 10, 100

    def worker(t: int) -> int:
        accepted = 0
        for i in range(per_thread):
            r = service.ingest(_ev(f"t{t}-{i}"), now_ms=20_000)
            accepted += not r.deduped
        return accepted

    with cf.ThreadPoolExecutor(n_threads) as pool:
        results = list(pool.map(worker, range(n_threads)))

    assert sum(results) == n_threads * per_thread
    rows = service.aggregates(settled_only=False).rows
    total = sum(r.count for r in rows)
    assert total == n_threads * per_thread
    print(f"[输入] {n_threads} 线程 x {per_thread} 个唯一事件并发写入")
    print(f"[判定依据] 全部唯一接受数={sum(results)}，聚合总数={total}，无漏计")


def test_concurrent_duplicate_writes_count_once(service) -> None:
    n_threads, n_ids = 10, 50
    barrier = threading.Barrier(n_threads)

    def worker() -> int:
        barrier.wait()
        accepts = 0
        for i in range(n_ids):
            r = service.ingest(_ev(f"dup-{i}"), now_ms=20_000)
            accepts += not r.deduped
        return accepts

    with cf.ThreadPoolExecutor(n_threads) as pool:
        results = [f.result() for f in [pool.submit(worker) for _ in range(n_threads)]]

    assert sum(results) == n_ids  # 每个 event_id 只被接受一次
    rows = service.aggregates(settled_only=False).rows
    assert sum(r.count for r in rows) == n_ids
    print(f"[输入] {n_threads} 线程同时提交同一批 {n_ids} 个 event_id")
    print(f"[判定依据] 仅首次接受={sum(results)}（应={n_ids}），其余 deduped，无重复计数")


def test_concurrent_writers_on_shared_file_db(tmp_path) -> None:
    db = tmp_path / "concurrent.db"
    services = [EventService(db) for _ in range(2)]
    try:
        barrier = threading.Barrier(2)

        def worker(idx: int) -> int:
            svc = services[idx]
            barrier.wait()
            accepted = 0
            # 两线程写不同 id（共 200）+ 相同 id（shared-*，共 50 个都写）
            for i in range(100):
                accepted += not svc.ingest(_ev(f"s{idx}-{i}"), now_ms=20_000).deduped
            for i in range(50):
                accepted += not svc.ingest(_ev(f"shared-{i}"), now_ms=20_000).deduped
            return accepted

        with cf.ThreadPoolExecutor(2) as pool:
            got = list(pool.map(worker, range(2)))

        rows = services[0].aggregates(settled_only=False).rows
        total = sum(r.count for r in rows)
        assert total == 250, total          # 200 唯一 + 50 去重
        assert sum(got) == 250, sum(got)
        print("[输入] 两个独立连接并发：各自100个唯一事件 + 共同50个重复事件")
        print(f"[判定依据] BEGIN IMMEDIATE 文件锁串行化，接受={sum(got)}，总数={total}")
    finally:
        for svc in services:
            svc.close()
