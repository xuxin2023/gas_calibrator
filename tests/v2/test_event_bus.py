from gas_calibrator.v2.core.event_bus import EventBus, EventType


def test_event_bus_subscribe_publish_and_unsubscribe() -> None:
    bus = EventBus()
    received: list[tuple[EventType, object]] = []

    def handler(event) -> None:
        received.append((event.type, event.data))

    bus.subscribe(EventType.POINT_STARTED, handler)
    bus.publish(EventType.POINT_STARTED, {"index": 1})
    bus.unsubscribe(EventType.POINT_STARTED, handler)
    bus.publish(EventType.POINT_STARTED, {"index": 2})

    assert received == [(EventType.POINT_STARTED, {"index": 1})]


def test_event_bus_clear_removes_all_handlers() -> None:
    bus = EventBus()
    called: list[int] = []

    def handler(event) -> None:
        called.append(1)

    bus.subscribe(EventType.WARNING_RAISED, handler)
    bus.clear()
    bus.publish(EventType.WARNING_RAISED, None)

    assert called == []
