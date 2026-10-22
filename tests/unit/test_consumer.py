import json

import pytest

import src.streaming.consumer as consumer_module


def test_database_failure_prevents_kafka_offset_commit(monkeypatch):
    event = {"payload": {"op": "d", "after": None}}

    class FakeMessage:
        def value(self):
            return json.dumps(event).encode("utf-8")

        def error(self):
            return None

        def key(self):
            return None

        def topic(self):
            return "streamguard.public.transactions"

        def partition(self):
            return 0

        def offset(self):
            return 42

    class FakeConnection:
        closed = False

        def close(self):
            self.closed = True

    class FakeConsumer:
        def __init__(self, config):
            self.committed = False
            self.closed = False

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            return FakeMessage()

        def commit(self, **kwargs):
            self.committed = True

        def close(self):
            self.closed = True

    fake_connection = FakeConnection()
    fake_consumer = FakeConsumer({})

    monkeypatch.setattr(
        consumer_module,
        "Consumer",
        lambda config: fake_consumer,
    )
    monkeypatch.setattr(
        consumer_module,
        "connect",
        lambda: fake_connection,
    )

    def fail_persistence(*args, **kwargs):
        raise RuntimeError("Simulated database failure")

    monkeypatch.setattr(
        consumer_module,
        "persist_event",
        fail_persistence,
    )

    with pytest.raises(RuntimeError, match="Simulated database failure"):
        consumer_module.main()

    assert fake_consumer.committed is False
    assert fake_consumer.closed is True
    assert fake_connection.closed is True


def test_duplicate_event_is_not_processed_as_new_and_offset_is_committed(monkeypatch, capsys):
    event = {"payload": {"op": "d", "after": None}}

    class FakeMessage:
        def value(self):
            return json.dumps(event).encode("utf-8")

        def error(self):
            return None

        def key(self):
            return None

        def topic(self):
            return "streamguard.public.transactions"

        def partition(self):
            return 0

        def offset(self):
            return 43

    class FakeConnection:
        closed = False

        def close(self):
            self.closed = True

    class FakeConsumer:
        def __init__(self):
            self.committed = False
            self.closed = False

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            if self.committed:
                raise KeyboardInterrupt
            return FakeMessage()

        def commit(self, **kwargs):
            self.committed = True

        def close(self):
            self.closed = True

    fake_connection = FakeConnection()
    fake_consumer = FakeConsumer()
    persistence_calls = []

    monkeypatch.setattr(consumer_module, "Consumer", lambda config: fake_consumer)
    monkeypatch.setattr(consumer_module, "connect", lambda: fake_connection)

    def duplicate_persistence(*args, **kwargs):
        persistence_calls.append(kwargs)
        return False

    monkeypatch.setattr(consumer_module, "persist_event", duplicate_persistence)

    consumer_module.main()

    output = capsys.readouterr().out

    assert len(persistence_calls) == 1
    assert "Duplicate event ignored" in output
    assert fake_consumer.committed is True
    assert "Offset committed successfully." in output
    assert fake_consumer.closed is True
    assert fake_connection.closed is True


def test_successful_persistence_happens_before_offset_commit(monkeypatch, capsys):
    event = {
        "payload": {
            "op": "d",
            "after": None,
        }
    }
    execution_order = []

    class FakeMessage:
        def value(self):
            return json.dumps(event).encode("utf-8")

        def error(self):
            return None

        def key(self):
            return None

        def topic(self):
            return "streamguard.public.transactions"

        def partition(self):
            return 0

        def offset(self):
            return 44

    class FakeConnection:
        closed = False

        def close(self):
            self.closed = True

    class FakeConsumer:
        def __init__(self):
            self.committed = False
            self.closed = False

        def subscribe(self, topics):
            pass

        def poll(self, timeout):
            if self.committed:
                raise KeyboardInterrupt
            return FakeMessage()

        def commit(self, **kwargs):
            execution_order.append("commit")
            self.committed = True

        def close(self):
            self.closed = True

    fake_connection = FakeConnection()
    fake_consumer = FakeConsumer()

    monkeypatch.setattr(consumer_module, "Consumer", lambda config: fake_consumer)
    monkeypatch.setattr(consumer_module, "connect", lambda: fake_connection)

    def successful_persistence(*args, **kwargs):
        execution_order.append("persist")
        return True

    monkeypatch.setattr(consumer_module, "persist_event", successful_persistence)

    consumer_module.main()

    capsys.readouterr()

    assert execution_order == ["persist", "commit"]
    assert fake_consumer.committed is True
    assert fake_consumer.closed is True
    assert fake_connection.closed is True
