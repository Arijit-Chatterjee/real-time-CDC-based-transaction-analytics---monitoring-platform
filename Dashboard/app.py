from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable


# Locate the project independently of the current working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
GOLD_ROOT = PROJECT_ROOT / "lake" / "gold"

FACT_PATH = GOLD_ROOT / "fact_transactions"
MART_PATH = GOLD_ROOT / "mart_merchant_performance"

REFRESH_SECONDS = 15

FACT_COLUMNS = [
    "transaction_id",
    "customer_key",
    "merchant_key",
    "amount",
    "currency",
    "transaction_status",
    "transaction_region",
    "event_time",
    "is_high_value_inr",
    "ingested_at",
]

MART_COLUMNS = [
    "merchant_key",
    "currency",
    "transaction_count",
    "unique_customer_count",
    "approved_count",
    "declined_count",
    "approval_rate_pct",
    "decline_rate_pct",
    "total_amount",
    "high_value_inr_count",
    "review_priority_rank",
]


st.set_page_config(
    page_title="StreamGuard Analytics",
    page_icon="🛡️",
    layout="wide",
)

st.markdown(
    """
    <style>
        .block-container {
            padding-top: 1.6rem;
            padding-bottom: 2rem;
        }
        div[data-testid="stMetric"] {
            background: rgba(128, 128, 128, 0.07);
            border: 1px solid rgba(128, 128, 128, 0.18);
            padding: 14px;
            border-radius: 10px;
        }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_data(ttl=12, show_spinner=False)
def load_gold_data():
    """Read selected columns from the latest Gold Delta snapshots."""
    fact_table = DeltaTable(str(FACT_PATH))
    mart_table = DeltaTable(str(MART_PATH))

    facts = fact_table.to_pandas(columns=FACT_COLUMNS)
    mart = mart_table.to_pandas(columns=MART_COLUMNS)

    facts["amount"] = pd.to_numeric(facts["amount"], errors="coerce")

    facts["event_time"] = pd.to_datetime(
        facts["event_time"], errors="coerce", utc=True
    ).dt.tz_convert("Asia/Kolkata")

    facts["ingested_at"] = pd.to_datetime(
        facts["ingested_at"], errors="coerce", utc=True
    ).dt.tz_convert("Asia/Kolkata")

    facts["transaction_status"] = facts["transaction_status"].fillna("Unknown").astype(str)
    facts["transaction_region"] = facts["transaction_region"].fillna("Unknown").astype(str)
    facts["currency"] = facts["currency"].fillna("Unknown").astype(str)
    facts["is_high_value_inr"] = facts["is_high_value_inr"].fillna(False).astype(bool)

    for column in (
        "transaction_count",
        "unique_customer_count",
        "approved_count",
        "declined_count",
        "high_value_inr_count",
        "review_priority_rank",
    ):
        if column in mart.columns:
            mart[column] = pd.to_numeric(mart[column], errors="coerce")

    return facts, mart


st.title("🛡️ StreamGuard Analytics")
st.caption("Live transaction monitoring · Gold Delta Lake · Synthetic data")


@st.fragment(run_every=f"{REFRESH_SECONDS}s")
def render_dashboard():
    try:
        with st.spinner("Reading Gold analytics..."):
            facts, mart = load_gold_data()
    except Exception as exc:
        st.error(
            "Could not read the Gold tables. Confirm that both Delta "
            "tables exist and that their files are accessible from Windows."
        )
        st.code(str(exc))
        st.caption(f"Transactions: {FACT_PATH}")
        st.caption(f"Merchant mart: {MART_PATH}")
        return

    if facts.empty:
        st.warning("The Gold transaction table contains no records.")
        return

    # Dashboard filters.
    currencies = sorted(facts["currency"].dropna().unique().tolist())
    statuses = sorted(facts["transaction_status"].dropna().unique().tolist())
    regions = sorted(facts["transaction_region"].dropna().unique().tolist())

    c1, c2, c3 = st.columns(3)

    with c1:
        currency_filter = st.selectbox(
            "Currency",
            ["All"] + currencies,
            key="dashboard_currency",
        )

    with c2:
        status_filter = st.selectbox(
            "Transaction status",
            ["All"] + statuses,
            key="dashboard_status",
        )

    with c3:
        region_filter = st.selectbox(
            "Region",
            ["All"] + regions,
            key="dashboard_region",
        )

    filtered = facts.copy()

    if currency_filter != "All":
        filtered = filtered[filtered["currency"] == currency_filter]

    if status_filter != "All":
        filtered = filtered[filtered["transaction_status"] == status_filter]

    if region_filter != "All":
        filtered = filtered[filtered["transaction_region"] == region_filter]

    st.caption(
        f"Last dashboard refresh: "
        f"{datetime.now().astimezone().strftime('%Y-%m-%d %H:%M:%S %Z')} "
        f"· Automatic refresh every {REFRESH_SECONDS} seconds"
    )

    if filtered.empty:
        st.info("No transactions match the current filters.")
        return

    # Normalize status only for calculations.
    normalized_status = filtered["transaction_status"].str.strip().str.lower()

    approved_count = int(normalized_status.eq("approved").sum())
    declined_count = int(normalized_status.isin(["declined", "decline"]).sum())

    approval_rate = approved_count / len(filtered) * 100
    high_value_count = int(filtered["is_high_value_inr"].sum())

    latest_event = filtered["event_time"].max()
    latest_ingestion = filtered["ingested_at"].max()

    st.subheader("Transaction overview")

    k1, k2, k3, k4 = st.columns(4)

    k1.metric("Transactions", f"{len(filtered):,}")
    k2.metric("Approval rate", f"{approval_rate:.1f}%")
    k3.metric("Declined transactions", f"{declined_count:,}")
    k4.metric("High-value INR", f"{high_value_count:,}")

    m1, m2 = st.columns(2)

    with m1:
        st.metric(
            "Latest event time (IST)",
            latest_event.strftime("%d %b %Y, %H:%M:%S")
            if pd.notna(latest_event)
            else "Unavailable",
        )

    with m2:
        st.metric(
            "Latest Gold ingestion (IST)",
            latest_ingestion.strftime("%d %b %Y, %H:%M:%S")
            if pd.notna(latest_ingestion)
            else "Unavailable",
        )

    st.divider()

    left, right = st.columns(2)

    with left:
        st.subheader("Transaction activity")

        daily = (
            filtered.dropna(subset=["event_time"])
            .assign(event_date=lambda df: df["event_time"].dt.date)
            .groupby("event_date")
            .size()
            .rename("Transactions")
            .sort_index()
        )

        if daily.empty:
            st.info("No valid event timestamps to chart.")
        else:
            st.line_chart(daily)

    with right:
        st.subheader("Transactions by currency")

        currency_counts = (
            filtered.groupby("currency").size().rename("Transactions").sort_values(ascending=False)
        )

        st.bar_chart(currency_counts)

    st.subheader("Monetary totals by currency")

    # Keep currencies separate: INR and USD are not added together.
    currency_summary = (
        filtered.groupby("currency", dropna=False)
        .agg(
            transaction_count=("transaction_id", "count"),
            total_amount=("amount", "sum"),
            average_amount=("amount", "mean"),
        )
        .reset_index()
    )

    currency_summary["total_amount"] = currency_summary["total_amount"].round(2)
    currency_summary["average_amount"] = currency_summary["average_amount"].round(2)

    st.dataframe(
        currency_summary,
        hide_index=True,
        width="stretch",
        column_config={
            "currency": "Currency",
            "transaction_count": st.column_config.NumberColumn("Transactions", format="%d"),
            "total_amount": st.column_config.NumberColumn("Total amount", format="%.2f"),
            "average_amount": st.column_config.NumberColumn("Average amount", format="%.2f"),
        },
    )

    st.divider()

    st.subheader("Merchant review priorities")

    merchant_view = mart.copy()

    # The mart is aggregated by merchant and currency.
    # Only the currency filter can be applied faithfully here.
    if currency_filter != "All":
        merchant_view = merchant_view[merchant_view["currency"] == currency_filter]

    if "review_priority_rank" in merchant_view.columns:
        merchant_view = merchant_view.sort_values(
            "review_priority_rank",
            ascending=True,
            na_position="last",
        )

    if merchant_view.empty:
        st.info("No merchant groups match this currency.")
    else:
        merchant_display_columns = [
            column
            for column in [
                "review_priority_rank",
                "merchant_key",
                "currency",
                "transaction_count",
                "decline_rate_pct",
                "high_value_inr_count",
                "approval_rate_pct",
                "total_amount",
            ]
            if column in merchant_view.columns
        ]

        st.dataframe(
            merchant_view[merchant_display_columns].head(20),
            hide_index=True,
            width="stretch",
        )

    st.caption(
        "Merchant rankings are rule-based review priorities, not confirmed "
        "fraud labels. Merchant totals use the complete mart data for the "
        "selected currency; transaction status and region filters do not "
        "change the mart aggregates."
    )

    st.divider()

    st.subheader("Latest transactions")

    display_columns = [
        column
        for column in [
            "event_time",
            "transaction_id",
            "merchant_key",
            "amount",
            "currency",
            "transaction_status",
            "transaction_region",
            "is_high_value_inr",
        ]
        if column in filtered.columns
    ]

    latest = filtered.sort_values("event_time", ascending=False, na_position="last").head(20)

    st.dataframe(
        latest[display_columns],
        hide_index=True,
        width="stretch",
    )


render_dashboard()
