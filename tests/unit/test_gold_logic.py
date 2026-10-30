import pytest

from src.gold.logic import (
    should_use_insert_only_append,
    transaction_range_merge_condition,
)


def test_transaction_range_merge_condition():
    result = transaction_range_merge_condition(
        "aaa",
        "zzz",
    )

    assert result == (
        "t.transaction_id >= 'aaa' "
        "AND t.transaction_id <= 'zzz' "
        "AND t.transaction_id = s.transaction_id"
    )


def test_transaction_range_condition_escapes_quotes():
    result = transaction_range_merge_condition(
        "id'one",
        "id'two",
    )

    assert "'id''one'" in result
    assert "'id''two'" in result


@pytest.mark.parametrize(
    ("minimum_id", "maximum_id"),
    [
        (None, "zzz"),
        ("aaa", None),
        ("", "zzz"),
        ("aaa", ""),
    ],
)
def test_transaction_range_rejects_missing_bounds(
    minimum_id,
    maximum_id,
):
    with pytest.raises(ValueError):
        transaction_range_merge_condition(minimum_id, maximum_id)


def test_insert_only_append_when_batch_contains_only_inserts():
    assert should_use_insert_only_append(
        has_non_insert_event=False,
        has_deletes=False,
    )


@pytest.mark.parametrize(
    ("has_non_insert_event", "has_deletes"),
    [
        (True, False),
        (False, True),
        (True, True),
    ],
)
def test_merge_required_for_updates_or_deletes(
    has_non_insert_event,
    has_deletes,
):
    assert not should_use_insert_only_append(
        has_non_insert_event,
        has_deletes,
    )
