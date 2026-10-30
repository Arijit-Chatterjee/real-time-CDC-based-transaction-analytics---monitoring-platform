"""Spark-independent logic used by the Gold incremental writer."""

from __future__ import annotations


def transaction_range_merge_condition(
    minimum_id: str | None,
    maximum_id: str | None,
) -> str:
    """Build a UUID range condition for an incremental fact merge."""

    if not minimum_id or not maximum_id:
        raise ValueError("Transaction ID bounds cannot be empty")

    minimum_literal = minimum_id.replace("'", "''")
    maximum_literal = maximum_id.replace("'", "''")

    return (
        f"t.transaction_id >= '{minimum_literal}' "
        f"AND t.transaction_id <= '{maximum_literal}' "
        "AND t.transaction_id = s.transaction_id"
    )


def should_use_insert_only_append(
    has_non_insert_event: bool,
    has_deletes: bool,
) -> bool:
    """Determine whether a transaction batch can use direct Delta append."""

    return not has_non_insert_event and not has_deletes
