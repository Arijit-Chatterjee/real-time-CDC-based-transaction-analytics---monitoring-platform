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

    st.subheader("Gold Delta Lake tables")

    gold_metadata = get_gold_table_metadata()

    st.dataframe(
        gold_metadata,
        hide_index=True,
        width="stretch",
    )

    st.caption(
        "Table versions and latest Delta commit timestamps are metadata. "
        "A new commit does not necessarily mean new business events arrived."
    )

    st.divider()

    # --------------------------------------------------------
    # Gold batch activity
    # --------------------------------------------------------

    st.subheader("Recent Gold pipeline activity")

    # --------------------------------------------------------
    # Live service logs
    # --------------------------------------------------------

    st.divider()
    st.subheader("Live Service Logs")

    st.caption(
        "Latest five log lines from the selected service. Automatically refreshed every 15 seconds."
    )

    selected_service = st.selectbox(
        "Select service",
        options=list(LOG_SOURCES.keys()),
        index=0,
        key="operations_log_source",
    )

    filename, log_output = get_recent_service_log(selected_service)

    st.caption(f"Log source: {filename}")

    # Highlight operational errors without hiding the original log entries.
    error_pattern = re.compile(
        r"\b(ERROR|FATAL|Exception|Traceback)\b",
        re.IGNORECASE,
    )

    log_lines = log_output.splitlines()
    has_errors = any(error_pattern.search(line) for line in log_lines)

    if has_errors:
        st.error("Potential errors detected in recent logs.")
    else:
        st.success("No obvious errors detected in the latest log lines.")

    st.code(
        "\n".join(log_lines[-10:]) or "No log entries available.",
        language="text",
    )

    st.caption(
        "This panel displays recent log output, not a complete error history. "
        "Older errors may no longer appear in the latest five lines."
    )
    st.caption(
        "Log activity is read-only. Historical log entries may remain visible "
        "after a writer restart; check timestamps before interpreting health."
    )


render_operations()
