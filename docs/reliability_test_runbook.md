# StreamGuard Reliability Test Record

Last updated: 2026-10-29

This document records manual reliability tests performed against the local
Docker-based StreamGuard environment. Results describe the specific scenarios
tested and are not guarantees for every production workload or failure mode.

## Verified scenarios

| Scenario | Observed result |
|---|---|
| New customer | Customer 10001 reached Gold `dim_customer`. |
| New merchant | Merchant 501 reached Gold `dim_merchant`. |
| Customer update | The changed customer region eventually appeared in Gold. |
| Merchant update | The changed merchant name and region appeared in Gold. |
| Transaction using new dimension keys | A Gold fact row resolved customer key 10001 and merchant key 501. |
| Transaction update | The existing fact row changed to amount 1999.99, status `declined`, and region `Howrah`. Exactly one row remained. |
| Transaction deletion | The deleted transaction eventually had zero matching rows in Gold. |
| Two-transaction burst | One Gold micro-batch processed two rows; both appeared in the dashboard. |
| Five-transaction burst | One Gold micro-batch processed five rows; all five appeared in the dashboard. |
| Gold restart catch-up | A transaction inserted while Gold was stopped was processed after restart using the existing checkpoint and appeared exactly once in Gold. |
| Delta append retry | An isolated temporary-table test repeated an append using the same transaction application ID and version. The row count remained one. |
| Live fact-table uniqueness | At the time of the check, 100,038 rows had 100,038 distinct transaction IDs and zero duplicate-key rows. |

## Representative batch result

Gold batch 23 processed five transaction rows using the insert-only append path.

- Rows processed: 5
- Logged processing duration: 20.55 seconds
- Dashboard: all five transactions were visible

These timings came from a local computer running Docker Desktop. They are not production performance guarantees.

## Recovery and retry limitations

- The restart test used a normal Gold stop, inserted a transaction while Gold was offline, and restarted Gold with its existing checkpoint.
- The isolated Delta retry test checked duplicate-write protection using identical `txnAppId` and `txnVersion` values.
- A deliberate process crash between a committed Delta write and streaming checkpoint advancement has not been simulated.
- Automated integration tests for the complete CDC-to-Gold flow have not yet been added.
- The local customer 10001 and merchant 501 records are synthetic test records.

## Reproducing basic checks

1. Run the existing automated unit tests with `uv run pytest -q`.
2. Inspect the current Gold writer log after test inserts, updates, or deletes.
3. Verify expected records directly in the Gold Delta tables.
4. Check transaction-ID uniqueness after a test scenario.
5. Preserve the existing streaming checkpoints during normal restart tests.

See [`commands_to_know.md`](commands_to_know.md) for reusable verification commands.
