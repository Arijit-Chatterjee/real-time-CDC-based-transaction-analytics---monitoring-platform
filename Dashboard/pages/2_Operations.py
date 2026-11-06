"""StreamGuard Operations and infrastructure monitoring."""

from __future__ import annotations

import json
import re
import subprocess
import urllib.error
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import pandas as pd
import pyarrow.dataset as ds
import streamlit as st
from deltalake import DeltaTable

PROJECT_ROOT = Path(__file__).resolve().parents[2]
GOLD_ROOT = PROJECT_ROOT / "lake" / "gold"

REFRESH_SECONDS = 15

CORE_CONTAINERS = [
    "streamguard-postgres",
    "streamguard-kafka",
    "streamguard-debezium",
    "streamguard-spark",
]

OPTIONAL_CONTAINERS = [
    "streamguard-debezium-ui",
    "streamguard-kafka-ui",
    "streamguard-airflow",
]

GOLD_TABLES = [
    "fact_transactions",
    "dim_customer",
    "dim_merchant",
    "dim_date",
    "mart_merchant_performance",
]


def run_command(args: list[str], timeout: int = 8):
    """Run a system command with timeout and captured output."""
    try:
        return subprocess.run(
            args,
            capture_output=True,
            text=True,
            timeout=timeout,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None


def get_container_status(name: str) -> str:
    """Return the current Docker container state."""
    result = run_command(
        [
            "docker",
            "inspect",
            "--format",
            "{{.State.Status}}",
            name,
        ]
    )

    if result is None:
        return "unavailable"

    if result.returncode != 0:
        return "missing"

    return result.stdout.strip().lower() or "unknown"


def get_debezium_status() -> dict:
    """Read Debezium connector and task state."""
    url = "http://localhost:8083/connectors/streamguard-postgres-connector/status"

    try:
        with urllib.request.urlopen(url, timeout=5) as response:
            return json.loads(response.read().decode("utf-8"))

    except (urllib.error.URLError, TimeoutError, OSError, ValueError) as exc:
        return {"error": str(exc)}


def get_docker_stats() -> pd.DataFrame:
    """Read current Docker container CPU and memory statistics."""
    result = run_command(
        [
            "docker",
            "stats",
            "--no-stream",
            "--format",
            "{{json .}}",
        ]
    )

    if result is None or result.returncode != 0:
        return pd.DataFrame()

    rows = []

    for line in result.stdout.splitlines():
        try:
            item = json.loads(line)
            rows.append(
                {
                    "Container": item.get("Name"),
                    "CPU": item.get("CPUPerc"),
                    "Memory usage": item.get("MemUsage"),
                    "Memory %": item.get("MemPerc"),
                    "PIDs": item.get("PIDs"),
                }
            )
        except (ValueError, TypeError):
            continue

    return pd.DataFrame(rows)


def get_spark_writers() -> dict[str, list[str]]:
    """Find active Python streaming writer processes in Spark."""
    script = r"""
ps -eo comm=,args= | awk '
$1 == "python3" && /\/opt\/streamguard\/src\/streaming\/bronze_writer.py/ {
    print "Bronze: " $0
}
$1 == "python3" && /\/opt\/streamguard\/src\/silver\/silver_writer.py/ {
    print "Silver: " $0
}
$1 == "python3" && /\/opt\/streamguard\/src\/gold\/gold_incremental_writer.py/ {
    print "Gold: " $0
}'
"""

    result = run_command(
        [
            "docker",
            "exec",
            "streamguard-spark",
            "bash",
            "-lc",
            script,
        ]
    )

    output = result.stdout if result and result.returncode == 0 else ""

    writers = {
        "Bronze": [],
        "Silver": [],
        "Gold": [],
    }

    for line in output.splitlines():
        for name, processes in writers.items():
            if line.startswith(name + ":"):
                processes.append(line.strip())

    return writers


def get_latest_gold_log() -> tuple[str, str]:
    """Read recent batch activity from the latest Gold writer log."""
    script = r"""
latest=$(ls -t /tmp/gold_incremental_writer*.log 2>/dev/null | head -n 1)

if [ -n "$latest" ]; then
    echo "LOG_FILE=$latest"
    grep -E "transactions batch .*rows=|used the insert-only append path|completed in Gold|ERROR|Exception|Traceback" "$latest" | tail -n 12
else
    echo "No Gold writer log found."
fi
"""

    result = run_command(
        [
            "docker",
            "exec",
            "streamguard-spark",
            "bash",
            "-lc",
            script,
        ]
    )

    if result is None or result.returncode != 0:
        return "Unavailable", "Could not read Gold writer logs."

    output = result.stdout.strip()

    if not output:
        return "Unavailable", "No Gold log activity found."

    lines = output.splitlines()
    filename = "Unknown"

    if lines and lines[0].startswith("LOG_FILE="):
        filename = lines[0].split("=", 1)[1]
        lines = lines[1:]

    return filename, "\n".join(lines) or "No recent matching events."


LOG_SOURCES = {
    "Gold writer": (
        "file",
        "/tmp/gold_incremental_writer*.log",
    ),
    "Silver writer": (
        "file",
        "/tmp/silver_writer*.log",
    ),
    "Bronze transactions": (
        "file",
        "/tmp/bronze_transactions*.log*",
    ),
    "Bronze customers": (
        "file",
        "/tmp/bronze_customers*.log*",
    ),
    "Bronze merchants": (
        "file",
        "/tmp/bronze_merchants*.log*",
    ),
    "Debezium": ("container", "streamguard-debezium"),
    "Kafka": ("container", "streamguard-kafka"),
    "PostgreSQL": ("container", "streamguard-postgres"),
}


@st.cache_data(ttl=15, show_spinner=False)
def get_gold_batch_history(
    limit: int = 10,
) -> tuple[str, pd.DataFrame, str | None]:
    """Parse recent Gold transaction batch metrics from the latest writer log."""

    script = r"""
latest=$(ls -t /tmp/gold_incremental_writer*.log 2>/dev/null | head -n 1)

if [ -n "$latest" ]; then
    printf 'LOG_FILE=%s\n' "$latest"
    tail -n 500 "$latest"
else
    echo "No Gold writer log found."
fi
"""

    result = run_command(
        [
            "docker",
            "exec",
            "streamguard-spark",
            "bash",
            "-lc",
            script,
        ],
        timeout=8,
    )

    columns = [
        "Batch ID",
        "Status",
        "Completion time",
        "Rows",
        "Duration (seconds)",
        "Write path",
    ]

    if result is None or result.returncode != 0:
        return (
            "Unavailable",
            pd.DataFrame(columns=columns),
            ("Could not retrieve Gold writer logs."),
        )

    lines = result.stdout.splitlines()

    if not lines or not lines[0].startswith("LOG_FILE="):
        return "Unavailable", pd.DataFrame(columns=columns), ("No Gold writer log file was found.")

    filename = lines[0].split("=", 1)[1]

    records = {}

    for line in lines[1:]:
        batch_match = re.search(r"transactions batch (\d+)", line)

        if not batch_match:
            continue

        batch_id = int(batch_match.group(1))

        record = records.setdefault(
            batch_id,
            {
                "Batch ID": batch_id,
                "Status": "Not confirmed",
                "Completion time": "Unavailable",
                "Rows": None,
                "Duration (seconds)": None,
                "Write path": "Not identified",
            },
        )

        timestamp_match = re.match(
            r"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})",
            line,
        )

        if timestamp_match and "completed in Gold" in line:
            try:
                completed_at = (
                    datetime.strptime(
                        timestamp_match.group(1),
                        "%Y-%m-%d %H:%M:%S,%f",
                    )
                    .replace(tzinfo=UTC)
                    .astimezone()
                )

                record["Completion time"] = completed_at.strftime("%d %b %Y, %H:%M:%S %Z")
                record["Status"] = "Completed"

            except ValueError:
                record["Completion time"] = "Timestamp unavailable"

        rows_match = re.search(r"rows=(\d+)", line)

        if rows_match:
            record["Rows"] = int(rows_match.group(1))

        duration_match = re.search(
            r"""['"]total['"]\s*:\s*([0-9]+(?:\.[0-9]+)?)""",
            line,
        )

        if duration_match:
            record["Duration (seconds)"] = float(duration_match.group(1))

        if "used the insert-only append path" in line:
            record["Write path"] = "Insert-only append"
        elif "delete_merge" in line:
            record["Write path"] = "Delete merge"
        elif "fact_merge" in line:
            record["Write path"] = "Fact merge"
        elif "fact_append" in line:
            record["Write path"] = "Fact append"

    history = sorted(
        records.values(),
        key=lambda item: item["Batch ID"],
        reverse=True,
    )[:limit]

    return filename, pd.DataFrame(history, columns=columns), None


def get_recent_service_log(source: str) -> tuple[str, str]:
    """Return the latest five log lines for a selected service."""

    if source not in LOG_SOURCES:
        return "Unknown source", "Invalid log source."

    source_type, target = LOG_SOURCES[source]

    if source_type == "file":
        script = (
            f"latest=$(ls -t {target} 2>/dev/null | head -n 1); "
            'if [ -n "$latest" ]; then '
            'printf "LOG_FILE=%s\\n" "$latest"; '
            'tail -n 5 "$latest"; '
            'else echo "No matching log file found."; fi'
        )

        result = run_command(
            [
                "docker",
                "exec",
                "streamguard-spark",
                "bash",
                "-lc",
                script,
            ]
        )

        if result is None or result.returncode != 0:
            return target, "Could not retrieve Spark writer logs."

        lines = result.stdout.strip().splitlines()
        filename = target

        if lines and lines[0].startswith("LOG_FILE="):
            filename = lines.pop(0).split("=", 1)[1]

        return filename, "\n".join(lines) or "Log file is empty."

    result = run_command(
        [
            "docker",
            "logs",
            "--tail",
            "5",
            "--timestamps",
            target,
        ]
    )

    if result is None:
        return target, "Could not retrieve container logs."

    if result.returncode != 0:
        return target, result.stderr.strip() or "Container log retrieval failed."

    output = "\n".join(
        line for line in (result.stdout.splitlines() + result.stderr.splitlines()) if line
    )

    return target, output or "No recent log entries."


def get_recent_errors(limit: int = 20) -> tuple[list[str], list[str]]:
    """Collect recent errors from retained Spark and container logs."""

    error_pattern = re.compile(
        r"\b(ERROR|FATAL|Exception|Traceback)\b",
        re.IGNORECASE,
    )

    scan_lines = 2000
    records: list[tuple[float, str]] = []
    problems: list[str] = []

    def timestamp_key(line: str) -> float:
        """Extract a sortable timestamp from a log line."""
        match = re.search(
            r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"
            r"(?:[.,]\d+)?(?:Z|[+-]\d{2}:\d{2})?",
            line,
        )

        if not match:
            return 0.0

        try:
            value = match.group().replace("Z", "+00:00")
            timestamp = datetime.fromisoformat(value)

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

            return timestamp.timestamp()

        except (ValueError, OverflowError, OSError):
            return 0.0

    # Search Spark writer log files together in one Docker command.
    file_sources = [
        (name, target)
        for name, (source_type, target) in LOG_SOURCES.items()
        if source_type == "file"
    ]

    script_parts = []

    for name, target in file_sources:
        script_parts.append(
            f"for file in {target}; do "
            '[ -f "$file" ] || continue; '
            f'tail -n {scan_lines} "$file" | '
            'grep -Ei "ERROR|FATAL|Exception|Traceback" | '
            f'sed "s|^|[{name}] $file: |"; '
            "done"
        )

    if script_parts:
        result = run_command(
            [
                "docker",
                "exec",
                "streamguard-spark",
                "bash",
                "-lc",
                " ".join(script_parts) + "; true",
            ],
            timeout=5,
        )

        if result is None or result.returncode != 0:
            problems.append("Could not scan Spark writer log files.")
        else:
            for line in result.stdout.splitlines():
                if error_pattern.search(line):
                    records.append((timestamp_key(line), line))

    # Search retained Docker container logs.
    for name, (source_type, target) in LOG_SOURCES.items():
        if source_type != "container":
            continue

        result = run_command(
            [
                "docker",
                "logs",
                "--tail",
                str(scan_lines),
                "--timestamps",
                target,
            ],
            timeout=5,
        )

        if result is None:
            problems.append(f"Could not retrieve {name} logs.")
            continue

        if result.returncode != 0:
            problems.append(f"{name}: {result.stderr.strip()}")
            continue

        output = "\n".join(filter(None, [result.stdout, result.stderr]))

        for line in output.splitlines():
            if error_pattern.search(line):
                entry = f"[{name}] {line}"
                records.append((timestamp_key(line), entry))

    records.sort(key=lambda item: item[0])

    recent = [entry for _, entry in records[-limit:]]

    return recent, problems


def format_commit_time(value) -> str:
    """Format a Delta history timestamp for display."""
    try:
        if isinstance(value, (int, float)):
            timestamp = datetime.fromtimestamp(
                value / 1000,
                tz=UTC,
            )
        else:
            timestamp = datetime.fromisoformat(str(value))

            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=UTC)

        return timestamp.astimezone().strftime("%d %b %Y, %H:%M:%S")

    except (ValueError, TypeError, OSError, OverflowError):
        return str(value)


def get_gold_table_metadata() -> pd.DataFrame:
    """Read Gold Delta versions and latest commit timestamps."""
    rows = []

    for table_name in GOLD_TABLES:
        path = GOLD_ROOT / table_name

        if not path.exists():
            rows.append(
                {
                    "Table": table_name,
                    "Version": "Missing",
                    "Last commit": "Unavailable",
                }
            )
            continue

        try:
            table = DeltaTable(str(path))
            history = table.history(1)

            latest = history[0] if history else {}

            rows.append(
                {
                    "Table": table_name,
                    "Version": table.version(),
                    "Last commit": format_commit_time(latest.get("timestamp", "Unknown")),
                }
            )

        except (OSError, ValueError, RuntimeError) as exc:
            rows.append(
                {
                    "Table": table_name,
                    "Version": "Error",
                    "Last commit": str(exc),
                }
            )

    return pd.DataFrame(rows)


KAFKA_TOPICS = {
    "transactions": "streamguard.public.transactions",
    "customers": "streamguard.public.customers",
    "merchants": "streamguard.public.merchants",
}


def get_kafka_end_offsets(topic: str) -> tuple[dict[int, int], str | None]:
    """Read Kafka's latest next-offset position for every topic partition."""

    result = run_command(
        [
            "docker",
            "exec",
            "streamguard-kafka",
            "/opt/kafka/bin/kafka-get-offsets.sh",
            "--bootstrap-server",
            "localhost:9092",
            "--topic",
            topic,
            "--time",
            "-1",
        ],
        timeout=12,
    )

    if result is None:
        return {}, "Kafka offset command timed out or failed to execute."

    if result.returncode != 0:
        return {}, result.stderr.strip() or "Kafka offset query failed."

    offsets = {}

    for line in result.stdout.splitlines():
        parts = line.strip().rsplit(":", 2)

        if len(parts) != 3 or parts[0] != topic:
            continue

        try:
            partition = int(parts[1])
            end_offset = int(parts[2])
            offsets[partition] = end_offset
        except ValueError:
            continue

    if not offsets:
        return {}, "Kafka returned no partition offsets."

    return offsets, None


def get_max_persisted_offsets(
    table_path: Path,
    storage_format: str,
) -> tuple[dict[int, int], str | None]:
    """Read maximum persisted Kafka offsets without loading all columns."""

    try:
        if not table_path.exists():
            return {}, "Table path does not exist."

        if storage_format == "parquet":
            dataset = ds.dataset(str(table_path), format="parquet")
        elif storage_format == "delta":
            dataset = DeltaTable(str(table_path)).to_pyarrow_dataset()
        else:
            return {}, f"Unsupported storage format: {storage_format}"

        frame = dataset.to_table(columns=["kafka_partition", "kafka_offset"]).to_pandas()

        if frame.empty:
            return {}, None

        frame["kafka_partition"] = pd.to_numeric(frame["kafka_partition"], errors="coerce")

        frame["kafka_offset"] = pd.to_numeric(frame["kafka_offset"], errors="coerce")

        frame = frame.dropna(subset=["kafka_partition", "kafka_offset"])

        if frame.empty:
            return {}, None

        maxima = frame.groupby("kafka_partition")["kafka_offset"].max()

        return {int(partition): int(offset) for partition, offset in maxima.items()}, None

    except Exception as exc:  # noqa: BLE001
        # Keep one unreadable table from breaking the entire Operations page.
        return {}, str(exc)


@st.cache_data(ttl=30, show_spinner=False)
def get_pipeline_offset_lag() -> tuple[pd.DataFrame, list[str]]:
    """Compare Kafka, Bronze and Silver high-watermarks per partition."""

    rows = []
    warnings = []

    for table_name, topic in KAFKA_TOPICS.items():
        kafka_offsets, kafka_error = get_kafka_end_offsets(topic)

        if kafka_error:
            warnings.append(f"{table_name}: Kafka: {kafka_error}")
            continue

        bronze_offsets, bronze_error = get_max_persisted_offsets(
            PROJECT_ROOT / "lake" / "bronze" / table_name,
            "parquet",
        )

        silver_offsets, silver_error = get_max_persisted_offsets(
            PROJECT_ROOT / "lake" / "silver" / table_name,
            "delta",
        )

        if bronze_error:
            warnings.append(f"{table_name}: Bronze: {bronze_error}")

        if silver_error:
            warnings.append(f"{table_name}: Silver: {silver_error}")

        partitions = sorted(set(kafka_offsets) | set(bronze_offsets) | set(silver_offsets))

        for partition in partitions:
            kafka_end = kafka_offsets.get(partition)
            bronze_max = bronze_offsets.get(partition)
            silver_max = silver_offsets.get(partition)

            bronze_gap = (
                kafka_end - bronze_max - 1
                if kafka_end is not None and bronze_max is not None
                else None
            )

            silver_gap = (
                kafka_end - silver_max - 1
                if kafka_end is not None and silver_max is not None
                else None
            )

            if kafka_end is None:
                status = "Kafka offset unavailable"
            elif bronze_gap is None or silver_gap is None:
                status = "Incomplete offset data"
            elif bronze_gap < 0 or silver_gap < 0:
                status = "Watermark mismatch"
            elif bronze_gap > 0 or silver_gap > 0:
                status = "Processing backlog"
            else:
                status = "At Kafka high-watermark"

            rows.append(
                {
                    "Topic": table_name,
                    "Partition": partition,
                    "Kafka end": kafka_end,
                    "Bronze max": bronze_max,
                    "Bronze gap": bronze_gap,
                    "Silver max": silver_max,
                    "Silver gap": silver_gap,
                    "Status": status,
                }
            )

    return pd.DataFrame(rows), warnings


def get_pipeline_freshness() -> pd.DataFrame:
    """Read the latest Bronze, Silver and Gold storage updates."""

    now = datetime.now().astimezone()

    bronze_tables = ["transactions", "customers", "merchants"]
    silver_tables = ["transactions", "customers", "merchants"]

    table_groups = [
        ("Bronze", "parquet", bronze_tables),
        ("Silver", "delta", silver_tables),
        ("Gold", "delta", GOLD_TABLES),
    ]

    rows = []

    for layer, storage_format, table_names in table_groups:
        for table_name in table_names:
            path = PROJECT_ROOT / "lake" / layer.lower() / table_name

            try:
                if storage_format == "parquet":
                    files = path.rglob("*.parquet")
                    latest_file = max(
                        files,
                        key=lambda item: item.stat().st_mtime,
                        default=None,
                    )

                    if latest_file is None:
                        rows.append(
                            {
                                "Layer": layer,
                                "Table": table_name,
                                "Format": "Parquet",
                                "Last update": "No files found",
                                "Age": "Unavailable",
                                "Latest operation": "Unavailable",
                            }
                        )
                        continue

                    updated_at = datetime.fromtimestamp(latest_file.stat().st_mtime).astimezone()

                    operation = latest_file.name

                else:
                    if not path.exists():
                        rows.append(
                            {
                                "Layer": layer,
                                "Table": table_name,
                                "Format": "Delta",
                                "Last update": "Table missing",
                                "Age": "Unavailable",
                                "Latest operation": "Unavailable",
                            }
                        )
                        continue

                    table = DeltaTable(str(path))
                    history = table.history(1)
                    latest = history[0] if history else {}

                    raw_timestamp = latest.get("timestamp")

                    if raw_timestamp is None:
                        raise ValueError("Latest commit timestamp unavailable")

                    if isinstance(raw_timestamp, (int, float)):
                        updated_at = datetime.fromtimestamp(
                            raw_timestamp / 1000,
                            tz=UTC,
                        ).astimezone()
                    else:
                        updated_at = datetime.fromisoformat(str(raw_timestamp))

                        if updated_at.tzinfo is None:
                            updated_at = updated_at.replace(tzinfo=UTC)

                        updated_at = updated_at.astimezone()

                    operation = latest.get("operation", "Unknown")

                age_seconds = max(
                    0,
                    int((now - updated_at).total_seconds()),
                )

                if age_seconds < 60:
                    age = f"{age_seconds} seconds"
                elif age_seconds < 3600:
                    age = f"{age_seconds // 60} minutes"
                elif age_seconds < 86400:
                    age = f"{age_seconds // 3600} hours"
                else:
                    age = f"{age_seconds // 86400} days"

                rows.append(
                    {
                        "Layer": layer,
                        "Table": table_name,
                        "Format": storage_format.capitalize(),
                        "Last update": updated_at.strftime("%d %b %Y, %H:%M:%S"),
                        "Age": age,
                        "Latest operation": operation,
                    }
                )

            except (OSError, ValueError, RuntimeError) as exc:
                rows.append(
                    {
                        "Layer": layer,
                        "Table": table_name,
                        "Format": storage_format.capitalize(),
                        "Last update": "Error",
                        "Age": "Unavailable",
                        "Latest operation": str(exc),
                    }
                )

    return pd.DataFrame(rows)


st.title("StreamGuard Operations")
st.caption("Infrastructure monitoring · CDC health · Spark activity · Gold Delta Lake")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_operations():

    st.caption(
        f"Last checked: {datetime.now().astimezone().strftime('%d %b %Y, %H:%M:%S %Z')} "
        f"· Automatic refresh every {REFRESH_SECONDS} seconds"
    )

    # --------------------------------------------------------
    # Infrastructure
    # --------------------------------------------------------

    st.subheader("Infrastructure health")

    container_columns = st.columns(4)
    container_results = {}

    for index, name in enumerate(CORE_CONTAINERS):
        status = get_container_status(name)
        container_results[name] = status

        with container_columns[index]:
            healthy = status == "running"
            st.metric(
                name.replace("streamguard-", "").capitalize(),
                "Running" if healthy else status.capitalize(),
            )

    healthy_count = sum(status == "running" for status in container_results.values())

    if healthy_count == len(CORE_CONTAINERS):
        st.success(f"All {healthy_count} required infrastructure services are running.")
    else:
        st.error(f"{healthy_count}/{len(CORE_CONTAINERS)} required services are running.")

    with st.expander("Optional services"):
        for name in OPTIONAL_CONTAINERS:
            status = get_container_status(name)

            if status == "running":
                st.write(f"🟢 {name}: Running")
            else:
                st.write(f"⚪ {name}: {status} (optional)")

    st.divider()

    # --------------------------------------------------------
    # Debezium
    # --------------------------------------------------------

    st.subheader("Change Data Capture")

    connector = get_debezium_status()

    if "error" in connector:
        st.error(f"Could not retrieve Debezium status: {connector['error']}")

    else:
        connector_state = connector.get("connector", {}).get("state", "UNKNOWN")

        tasks = connector.get("tasks", [])
        task_states = [task.get("state", "UNKNOWN") for task in tasks]

        c1, c2, c3 = st.columns(3)

        c1.metric("Connector", connector_state)
        c2.metric("Tasks", len(tasks))
        c3.metric(
            "Running tasks",
            sum(state == "RUNNING" for state in task_states),
        )

        if (
            connector_state == "RUNNING"
            and task_states
            and all(state == "RUNNING" for state in task_states)
        ):
            st.success("Debezium connector and all reported tasks are running.")
        else:
            st.warning("Debezium connector or task status needs attention.")

    st.divider()

    # --------------------------------------------------------
    # Spark writers
    # --------------------------------------------------------

    st.subheader("Spark streaming writers")

    writers = get_spark_writers()

    writer_columns = st.columns(3)

    expected_counts = {
        "Bronze": 3,
        "Silver": 1,
        "Gold": 1,
    }

    for index, name in enumerate(("Bronze", "Silver", "Gold")):
        count = len(writers[name])
        expected = expected_counts[name]

        with writer_columns[index]:
            st.metric(
                f"{name} writers",
                f"{count}/{expected}",
            )

            if count == expected:
                st.caption("Expected processes detected")
            else:
                st.caption("Count differs from expected")

    with st.expander("Writer process details"):
        for name, processes in writers.items():
            st.write(f"**{name}**")

            if processes:
                for process in processes:
                    st.code(process)
            else:
                st.warning(f"No {name} Python writer detected.")

    st.divider()

    # --------------------------------------------------------
    # Docker resource usage
    # --------------------------------------------------------

    st.subheader("Docker resource usage")

    stats = get_docker_stats()

    if stats.empty:
        st.warning("Docker resource statistics are unavailable.")
    else:
        st.dataframe(
            stats,
            hide_index=True,
            width="stretch",
        )

    st.divider()

    # --------------------------------------------------------
    # Gold table metadata
    # --------------------------------------------------------

    st.subheader("Pipeline Data Freshness")

    st.caption(
        "Latest storage updates across Bronze, Silver and Gold. "
        "Age measures time since the last file modification or Delta commit; "
        "it does not independently indicate pipeline health."
    )

    freshness = get_pipeline_freshness()

    st.dataframe(
        freshness,
        hide_index=True,
        width="stretch",
    )

    st.caption(
        "Bronze uses Parquet file modification times. "
        "Silver and Gold use Delta transaction history."
    )

    # --------------------------------------------------------
    # Kafka offset and streaming progress
    # --------------------------------------------------------

    st.divider()
    st.subheader("Kafka → Bronze → Silver Progress")

    st.caption(
        "Latest Kafka offsets compared with persisted Bronze and Silver offsets. "
        "Refreshes using cached data every 30 seconds."
    )

    with st.spinner("Checking Kafka and storage offsets..."):
        offset_data, offset_warnings = get_pipeline_offset_lag()

    if offset_warnings:
        with st.expander("Offset monitoring warnings"):
            for warning in offset_warnings:
                st.warning(warning)

    if offset_data.empty:
        st.warning("Kafka offset information is currently unavailable.")

    else:
        caught_up = int(offset_data["Status"].eq("At Kafka high-watermark").sum())

        backlog = int(offset_data["Status"].eq("Processing backlog").sum())

        mismatches = int(offset_data["Status"].eq("Watermark mismatch").sum())

        incomplete = int(
            offset_data["Status"].eq("Incomplete offset data").sum()
            + offset_data["Status"].eq("Kafka offset unavailable").sum()
        )

        m1, m2, m3, m4 = st.columns(4)

        m1.metric("Partitions monitored", len(offset_data))
        m2.metric("Caught up", caught_up)
        m3.metric("Backlog", backlog)
        m4.metric("Data issues", mismatches + incomplete)

        if mismatches:
            st.error(f"{mismatches} partition(s) have offset watermark mismatches.")
        elif incomplete:
            st.warning(f"{incomplete} partition(s) have incomplete offset information.")
        elif backlog:
            st.warning(f"{backlog} partition(s) have an offset backlog.")
        else:
            st.success(
                "Bronze and Silver have reached Kafka's current "
                "high-watermark across all monitored partitions."
            )

        st.dataframe(
            offset_data,
            hide_index=True,
            width="stretch",
            column_config={
                "Topic": "CDC topic",
                "Partition": st.column_config.NumberColumn(
                    "Partition",
                    format="%d",
                ),
                "Kafka end": st.column_config.NumberColumn(
                    "Kafka end offset",
                    format="%d",
                ),
                "Bronze max": st.column_config.NumberColumn(
                    "Bronze max offset",
                    format="%d",
                ),
                "Bronze gap": st.column_config.NumberColumn(
                    "Bronze gap",
                    format="%d",
                ),
                "Silver max": st.column_config.NumberColumn(
                    "Silver max offset",
                    format="%d",
                ),
                "Silver gap": st.column_config.NumberColumn(
                    "Silver gap",
                    format="%d",
                ),
                "Status": "Partition health",
            },
        )

    st.caption(
        "Offset gaps are high-watermark differences, not proof of "
        "message-by-message completeness or exact processing latency. "
        "Kafka retention and topic recreation can also affect comparisons."
    )

    st.caption(
        "Table versions and latest Delta commit timestamps are metadata. "
        "A new commit does not necessarily mean new business events arrived."
    )

    st.divider()

    # --------------------------------------------------------
    # Gold batch activity
    # --------------------------------------------------------

    st.divider()
    st.subheader("Recent Gold Pipeline Activity")

    # --------------------------------------------------------
    # Gold execution monitoring
    # --------------------------------------------------------

    st.divider()
    st.subheader("Gold Execution Monitor")

    batch_log_file, batch_history, batch_error = get_gold_batch_history(limit=10)

    st.caption(f"Gold log source: {batch_log_file}")

    if batch_error:
        st.warning(batch_error)

    elif not batch_history.empty:
        completed_history = batch_history[batch_history["Status"] == "Completed"]

        if not completed_history.empty:
            latest = completed_history.iloc[0]

            m1, m2, m3, m4 = st.columns(4)

            m1.metric(
                "Last successful batch",
                str(latest["Batch ID"]),
            )

            m2.metric(
                "Rows processed",
                str(latest["Rows"]) if pd.notna(latest["Rows"]) else "Unknown",
            )

            m3.metric(
                "Duration",
                f"{latest['Duration (seconds)']:.2f}s"
                if pd.notna(latest["Duration (seconds)"])
                else "Unknown",
            )

            m4.metric(
                "Write path",
                str(latest["Write path"]),
            )

            st.success(
                f"Latest confirmed successful batch: "
                f"{latest['Batch ID']} · {latest['Completion time']}"
            )

        else:
            st.info("No completed Gold transaction batches were found.")

        st.subheader("Recent Gold batch history")

        st.dataframe(
            batch_history,
            hide_index=True,
            width="stretch",
            column_config={
                "Batch ID": st.column_config.NumberColumn(
                    "Batch ID",
                    format="%d",
                ),
                "Rows": st.column_config.NumberColumn(
                    "Rows processed",
                    format="%d",
                ),
                "Duration (seconds)": st.column_config.NumberColumn(
                    "Duration (seconds)",
                    format="%.2f",
                ),
            },
        )

    else:
        st.info("No Gold batch history is currently available.")

    st.caption(
        "Batch completion is determined from explicit completion log messages. "
        "This is a recent-history view, not a durable audit archive."
    )

    # --------------------------------------------------------
    # Recent Gold log output
    # --------------------------------------------------------

    gold_log_file, gold_activity = get_latest_gold_log()

    with st.expander("View recent Gold log lines"):
        st.caption(f"Log source: {gold_log_file}")
        st.code(gold_activity, language="text")

    st.caption(
        "Gold batch metrics are extracted from the most recent retained "
        "writer log, so history may be incomplete after log rotation."
    )

    # --------------------------------------------------------
    # Recent error history
    # --------------------------------------------------------

    st.divider()
    st.subheader("Recent Error History")

    st.caption(
        "Latest 20 matching errors across retained service logs. "
        "Searches up to 2,000 lines per source."
    )

    recent_errors, scan_problems = get_recent_errors(limit=20)

    if scan_problems:
        with st.expander("Log collection warnings"):
            for problem in scan_problems:
                st.warning(problem)

    if recent_errors:
        st.error(f"{len(recent_errors)} matching error entries found.")

        st.code(
            "\n\n".join(recent_errors),
            language="text",
        )
    elif scan_problems:
        st.warning("Some log sources could not be inspected.")
    else:
        st.success("No matching errors found in the scanned log windows.")

    st.caption(
        "Errors remain searchable while their source logs are retained. "
        "This is not a permanent error archive."
    )

    # --------------------------------------------------------
    # Live service logs
    # --------------------------------------------------------

    st.divider()
    st.subheader("Live Service Logs")

    st.caption(
        "Select a service to inspect its five latest log lines. "
        "Automatically refreshed every 15 seconds."
    )

    selected_service = st.selectbox(
        "Select service",
        options=list(LOG_SOURCES.keys()),
        index=0,
        key="operations_log_source",
    )

    filename, log_output = get_recent_service_log(selected_service)

    st.caption(f"Log source: {filename}")

    log_lines = log_output.splitlines()

    error_pattern = re.compile(
        r"\b(ERROR|FATAL|Exception|Traceback)\b",
        re.IGNORECASE,
    )

    has_errors = any(error_pattern.search(line) for line in log_lines)

    if has_errors:
        st.error("Potential errors detected in the selected service logs.")
    elif "Could not retrieve" in log_output or "No matching log file found" in log_output:
        st.warning("Log source could not be read or has no matching file.")
    else:
        st.success("No obvious errors in the displayed log lines.")

    if log_lines:
        st.code(
            "\n".join(log_lines[-5:]),
            language="text",
        )
    else:
        st.info("No log entries are currently available.")

    st.caption(
        "Only the latest five lines are displayed here. "
        "Use Recent Error History to search a larger retained log window."
    )


render_operations()
