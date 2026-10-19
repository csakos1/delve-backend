from __future__ import annotations

from delve.events import EventHub


class Clock:
    def __init__(self) -> None:
        self.t = 0.0

    def __call__(self) -> float:
        return self.t


def test_seq_and_replay() -> None:
    hub = EventHub()
    for i in range(5):
        hub.publish("x", {"i": i})
    assert hub.seq == 5
    got = hub.since(2)
    assert got is not None
    assert [e.seq for e in got] == [3, 4, 5]
    assert hub.since(5) == []
    assert hub.since(0) is not None


def test_resync_cases() -> None:
    hub = EventHub(maxlen=3)
    for i in range(5):
        hub.publish("x", {"i": i})
    assert hub.since(1) is None  # fell out of the ring buffer
    assert hub.since(2) is not None  # 3, 4, 5 are still there
    assert hub.since(9) is None  # from the future: server restarted
    assert hub.since(-1) is None
    assert hub.since(4, epoch="other") is None
    assert hub.since(4, epoch=hub.epoch) is not None


def test_age_limit() -> None:
    clock = Clock()
    hub = EventHub(max_age_s=10, clock=clock)
    hub.publish("old", {})
    clock.t = 20
    hub.publish("new", {})
    assert hub.since(0) is None  # the old event expired
    got = hub.since(1)
    assert got is not None
    assert [e.type for e in got] == ["new"]


async def test_subscribers_and_overflow() -> None:
    hub = EventHub(queue_size=2)
    fast, slow = hub.subscribe(), hub.subscribe()
    hub.publish("a", {})
    fast.queue.get_nowait()
    hub.publish("b", {})
    fast.queue.get_nowait()
    hub.publish("c", {})
    assert slow.overflowed and slow.closed.is_set()
    assert not fast.overflowed
    assert hub.subscribers == 1
    assert fast.queue.get_nowait().frame() == {"type": "c", "seq": 3, "data": {}}
    hub.unsubscribe(fast)
    assert hub.subscribers == 0
