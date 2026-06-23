"""Public dashboard for Streamlit Community Cloud, S3 edition.

Reads the dbt marts as Parquet files over HTTPS from a public S3 prefix. It does
not touch the VM or MotherDuck, so the link stays up regardless of the collector's
health or any trial status.

Secrets (Streamlit Cloud -> app settings -> Secrets, TOML):
    marts_base_url = "https://YOUR_BUCKET.s3.us-east-2.amazonaws.com/marts"

Locally you can instead set the env var MARTS_BASE_URL.
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Prediction-Market Surveillance", layout="wide")
st.title("Prediction-Market Surveillance")
st.caption("Live data-health + manipulation surveillance on a Polymarket feed. "
           "Marts served from S3; raw events stay on the collector.")


def _secret(name: str, default: str | None = None) -> str | None:
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:  # noqa: BLE001 - secrets file may be absent locally
        pass
    return os.environ.get(name.upper(), default)


BASE = (_secret("marts_base_url") or "").rstrip("/")


@st.cache_resource
def connect():
    if not BASE:
        st.error("No marts_base_url set. Add it in Streamlit secrets.")
        st.stop()
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    return con


con = connect()


@st.cache_data(ttl=60)
def mart(name: str, where: str = "") -> pd.DataFrame:
    """Read one mart Parquet file from S3; empty frame if it isn't published yet."""
    sql = f"SELECT * FROM read_parquet('{BASE}/{name}.parquet') {where}"
    try:
        return con.execute(sql).fetch_df()
    except Exception as exc:  # noqa: BLE001
        st.info(f"`{name}` not available yet ({exc.__class__.__name__}). "
                f"Has the batch published to S3?")
        return pd.DataFrame()


# --- Ingest health ---
summary = mart("fct_ingest_summary")
c1, c2, c3 = st.columns(3)
if not summary.empty:
    row = summary.iloc[0]
    c1.metric("Valid events", f"{int(row['valid_count']):,}")
    c2.metric("Dead-lettered", f"{int(row['dlq_count']):,}")
    c3.metric("Contract reject rate", f"{row['reject_rate'] * 100:.1f}%")

st.subheader("Why records were rejected")
reasons = mart("fct_dead_letter_reasons")
if not reasons.empty:
    st.bar_chart(reasons.set_index("reject_reason"))

# --- Coherence (sum of outcomes ~ 1) ---
st.subheader("Market coherence violations (outcomes not summing to ~1)")
incoherent = mart("fct_coherence", "where is_incoherent order by minute desc limit 50")
if not incoherent.empty:
    st.dataframe(incoherent[["market_id", "minute", "outcome_sum", "deviation"]],
                 use_container_width=True)
else:
    st.caption("No coherence violations in the current window.")

# --- Reconciliation: did the stream lie? ---
st.subheader("Stream vs batch reconciliation")
rec = mart("rec_stream_vs_batch")
if not rec.empty:
    counts = rec.groupby("reconciliation_status").size().rename("n").to_frame()
    st.bar_chart(counts)

# --- Calibration: was the market right? ---
st.subheader("Calibration vs real resolutions (Brier score, lower is better)")
cal = mart("fct_calibration")
if not cal.empty:
    st.metric("Mean Brier score", f"{cal['brier_score'].mean():.4f}",
              help=f"over {len(cal)} resolved outcomes")
    st.dataframe(
        cal.sort_values("brier_score", ascending=False)
           .head(50)[["market_id", "outcome", "final_price", "won", "brier_score"]],
        use_container_width=True,
    )
else:
    st.caption("No resolved markets scored yet (resolutions accumulate over days).")
