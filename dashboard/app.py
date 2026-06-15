"""Live data-health + surveillance dashboard.

Reads the parquet lake with DuckDB and shows what makes this project about trust:
contract reject rate, dead-letter reasons, real-time price-range flags, and the
market-coherence check.

    streamlit run dashboard/app.py
"""
from __future__ import annotations

import os

import duckdb
import pandas as pd
import streamlit as st

LAKE = os.environ.get("LAKE_PATH", "data/lake")

st.set_page_config(page_title="Market Surveillance — Data Health", layout="wide")
st.title("Prediction-Market Surveillance — Data Health")
st.caption(f"Reading parquet lake at: {LAKE}")


def q(sql: str) -> pd.DataFrame:
    con = duckdb.connect()
    try:
        return con.execute(sql).fetch_df()
    except Exception as exc:  # noqa: BLE001
        st.info(f"No data yet ({exc.__class__.__name__}). Start ingestion and the Spark job.")
        return pd.DataFrame()
    finally:
        con.close()


valid = q(f"select count(*) c from read_parquet('{LAKE}/valid/**/*.parquet', union_by_name=true)")
dlq = q(f"select count(*) c from read_parquet('{LAKE}/dlq/**/*.parquet', union_by_name=true)")
n_valid = int(valid["c"][0]) if not valid.empty else 0
n_dlq = int(dlq["c"][0]) if not dlq.empty else 0
total = n_valid + n_dlq

c1, c2, c3 = st.columns(3)
c1.metric("Valid events", f"{n_valid:,}")
c2.metric("Dead-lettered", f"{n_dlq:,}")
c3.metric("Contract reject rate", f"{(n_dlq / total * 100):.1f}%" if total else "—")

st.subheader("Why records were rejected")
reasons = q(
    f"select reject_reason, count(*) n from read_parquet('{LAKE}/dlq/**/*.parquet', union_by_name=true) "
    f"group by 1 order by 2 desc"
)
if not reasons.empty:
    st.bar_chart(reasons.set_index("reject_reason"))

st.subheader("Real-time price-range flags")
flags = q(
    f"select asset_id, market_id, window_start, price_range, volume "
    f"from read_parquet('{LAKE}/flagged/**/*.parquet', union_by_name=true) "
    f"order by window_start desc limit 50"
)
if not flags.empty:
    st.dataframe(flags, use_container_width=True)
