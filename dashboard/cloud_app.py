"""Public dashboard for Streamlit Community Cloud.

Reads the dbt marts from MotherDuck (NOT the raw lake), so it can be hosted in
the cloud and shared as a public URL while the VM keeps all raw data private.

Secrets (set in the Streamlit Cloud app settings -> Secrets, TOML):
    motherduck_token = "..."     # a READ-ONLY / read-scaling token, not your write token
    md_database = "market_surveillance"

Locally you can instead export MOTHERDUCK_TOKEN and MD_DATABASE.
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

st.set_page_config(page_title="Prediction-Market Surveillance", layout="wide")
st.title("Prediction-Market Surveillance")
st.caption("Live data-health + manipulation surveillance on a Polymarket feed. "
           "Marts served from MotherDuck; raw events stay on the collector.")


def _secret(name: str, default: str | None = None) -> str | None:
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:  # noqa: BLE001 - secrets file may be absent locally
        pass
    return os.environ.get(name.upper(), default)


@st.cache_resource
def connect():
    token = _secret("motherduck_token")
    db = _secret("md_database", "market_surveillance")
    if not token:
        st.error("No MotherDuck token found. Set `motherduck_token` in Streamlit secrets.")
        st.stop()
    return duckdb.connect(f"md:{db}", config={"motherduck_token": token})


con = connect()


@st.cache_data(ttl=60)
def q(sql: str) -> pd.DataFrame:
    try:
        return con.execute(sql).fetch_df()
    except Exception as exc:  # noqa: BLE001
        st.info(f"`{sql.split()[3] if len(sql.split()) > 3 else sql}` not available yet "
                f"({exc.__class__.__name__}). Has the dbt batch run against MotherDuck?")
        return pd.DataFrame()


# --- Ingest health ---
summary = q("select * from fct_ingest_summary")
c1, c2, c3 = st.columns(3)
if not summary.empty:
    row = summary.iloc[0]
    c1.metric("Valid events", f"{int(row['valid_count']):,}")
    c2.metric("Dead-lettered", f"{int(row['dlq_count']):,}")
    c3.metric("Contract reject rate", f"{row['reject_rate'] * 100:.1f}%")

st.subheader("Why records were rejected")
reasons = q("select * from fct_dead_letter_reasons")
if not reasons.empty:
    st.bar_chart(reasons.set_index("reject_reason"))

# --- Coherence (Σ outcomes ≈ 1) ---
st.subheader("Market coherence violations (outcomes not summing to ~1)")
incoherent = q(
    "select market_id, minute, outcome_sum, deviation "
    "from fct_coherence where is_incoherent order by minute desc limit 50"
)
if not incoherent.empty:
    st.dataframe(incoherent, use_container_width=True)
else:
    st.caption("No coherence violations in the current window.")

# --- Reconciliation: did the stream lie? ---
st.subheader("Stream vs batch reconciliation")
rec = q("select reconciliation_status, count(*) n from rec_stream_vs_batch group by 1")
if not rec.empty:
    st.bar_chart(rec.set_index("reconciliation_status"))

# --- Calibration: was the market right? ---
st.subheader("Calibration vs real resolutions (Brier score, lower is better)")
cal = q("select round(avg(brier_score), 4) as mean_brier, count(*) as n from fct_calibration")
if not cal.empty and cal.iloc[0]["n"]:
    st.metric("Mean Brier score", f"{cal.iloc[0]['mean_brier']}", help=f"over {int(cal.iloc[0]['n'])} resolved outcomes")
    detail = q("select market_id, outcome, final_price, won, brier_score "
               "from fct_calibration order by brier_score desc limit 50")
    if not detail.empty:
        st.dataframe(detail, use_container_width=True)
else:
    st.caption("No resolved markets scored yet (resolutions accumulate over days).")
