"""Public dashboard for Streamlit Community Cloud, S3 edition (finance-terminal theme).

Reads the dbt marts as Parquet files over HTTPS from a public S3 prefix. It does
not touch the VM or MotherDuck, so the link stays up regardless of the collector's
health or any trial status.

Secrets (Streamlit Cloud -> app settings -> Secrets, TOML):
    marts_base_url = "https://YOUR_BUCKET.s3.us-east-1.amazonaws.com/marts"

Locally you can instead set the env var MARTS_BASE_URL.
"""
from __future__ import annotations

import datetime
import email.utils
import os
import urllib.request

import duckdb
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

# --------------------------------------------------------------------------- #
# Page + theme
# --------------------------------------------------------------------------- #
st.set_page_config(
    page_title="Prediction-Market Surveillance",
    page_icon="📡",
    layout="wide",
)

INK = "#0B0E14"
PANEL = "#141A24"
BORDER = "#243044"
TEXT = "#E6EDF3"
MUTED = "#8B98A9"
AMBER = "#E8B339"
GREEN = "#3FB950"
RED = "#F85149"
BLUE = "#58A6FF"

st.markdown(
    f"""
    <style>
      @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500;600&display=swap');

      html, body, [class*="css"] {{ font-family: 'Inter', sans-serif; }}
      #MainMenu, footer, header {{ visibility: hidden; }}
      [data-testid="stToolbar"] {{ display: none; }}
      .block-container {{ padding-top: 2.2rem; padding-bottom: 3rem; max-width: 1200px; }}

      .masthead {{
        border-bottom: 1px solid {BORDER}; padding-bottom: 1.1rem; margin-bottom: 1.6rem;
      }}
      .masthead .eyebrow {{
        font-family: 'IBM Plex Mono', monospace; font-size: 0.72rem; letter-spacing: 0.18em;
        text-transform: uppercase; color: {AMBER}; margin-bottom: 0.35rem;
      }}
      .masthead h1 {{
        font-size: 1.9rem; font-weight: 700; color: {TEXT}; margin: 0 0 0.5rem 0; letter-spacing: -0.01em;
      }}
      .masthead p {{ color: {MUTED}; font-size: 0.95rem; line-height: 1.55; max-width: 760px; margin: 0; }}
      .live-pill {{
        display: inline-flex; align-items: center; gap: 0.4rem; font-family: 'IBM Plex Mono', monospace;
        font-size: 0.7rem; letter-spacing: 0.1em; color: {GREEN}; border: 1px solid {BORDER};
        border-radius: 999px; padding: 0.2rem 0.6rem; margin-left: 0.6rem; vertical-align: middle;
      }}
      .live-dot {{ width: 7px; height: 7px; border-radius: 50%; background: {GREEN}; box-shadow: 0 0 6px {GREEN}; }}

      .kpi {{
        background: {PANEL}; border: 1px solid {BORDER}; border-radius: 10px; padding: 1.1rem 1.2rem; height: 100%;
      }}
      .kpi .label {{
        font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; letter-spacing: 0.12em;
        text-transform: uppercase; color: {MUTED}; margin-bottom: 0.5rem;
      }}
      .kpi .value {{ font-family: 'IBM Plex Mono', monospace; font-size: 1.85rem; font-weight: 600; color: {TEXT}; line-height: 1.1; }}
      .kpi .sub {{ font-size: 0.78rem; color: {MUTED}; margin-top: 0.35rem; }}
      .kpi .value.good {{ color: {GREEN}; }}
      .kpi .value.warn {{ color: {AMBER}; }}
      .kpi .value.bad {{ color: {RED}; }}

      .section-title {{
        font-size: 1.05rem; font-weight: 600; color: {TEXT}; margin: 2rem 0 0.2rem 0;
        display: flex; align-items: center; gap: 0.5rem;
      }}
      .section-note {{ color: {MUTED}; font-size: 0.85rem; margin-bottom: 0.6rem; }}
      .foot {{
        border-top: 1px solid {BORDER}; margin-top: 2.6rem; padding-top: 1rem;
        color: {MUTED}; font-size: 0.8rem; line-height: 1.6;
      }}
      .foot a {{ color: {BLUE}; text-decoration: none; }}
    </style>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------- #
# Data access
# --------------------------------------------------------------------------- #
def _secret(name: str, default: str | None = None) -> str | None:
    try:
        if name in st.secrets:
            return st.secrets[name]
    except Exception:  # noqa: BLE001
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
def mart(name: str) -> pd.DataFrame:
    try:
        return con.execute(f"SELECT * FROM read_parquet('{BASE}/{name}.parquet')").fetch_df()
    except Exception:  # noqa: BLE001
        return pd.DataFrame()


@st.cache_data(ttl=60)
def marts_age_minutes() -> float | None:
    """Age of the published marts, from the S3 object's Last-Modified header.

    This is the one honest freshness signal: a green build that never reaches S3
    leaves this old (the failure that froze this dashboard for days). The badge
    turns red so silent staleness can never look LIVE again.
    """
    try:
        req = urllib.request.Request(f"{BASE}/fct_ingest_summary.parquet", method="HEAD")
        with urllib.request.urlopen(req, timeout=10) as r:  # noqa: S310
            lm = r.headers.get("Last-Modified")
        if not lm:
            return None
        dt = email.utils.parsedate_to_datetime(lm)
        now = datetime.datetime.now(datetime.timezone.utc)
        return (now - dt).total_seconds() / 60.0
    except Exception:  # noqa: BLE001
        return None


def freshness_pill() -> str:
    age = marts_age_minutes()
    if age is None:
        color, label = MUTED, "DATA AGE UNKNOWN"
    elif age <= 30:
        color, label = GREEN, f"LIVE &middot; {int(age)}m ago"
    elif age <= 90:
        color, label = AMBER, f"{int(age)}m old"
    else:
        color, label = RED, f"STALE &middot; {age / 60:.1f}h old"
    return (
        f'<span class="live-pill" style="color:{color}">'
        f'<span class="live-dot" style="background:{color};box-shadow:0 0 6px {color}"></span>'
        f"{label}</span>"
    )


def plotly_shell(fig: go.Figure, height: int = 300) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        height=height,
        margin=dict(l=10, r=10, t=10, b=10),
        font=dict(family="IBM Plex Mono, monospace", size=12, color=TEXT),
        showlegend=False,
    )
    fig.update_xaxes(gridcolor=BORDER, zerolinecolor=BORDER)
    fig.update_yaxes(gridcolor=BORDER, zerolinecolor=BORDER)
    return fig


# --------------------------------------------------------------------------- #
# Load marts
# --------------------------------------------------------------------------- #
summary = mart("fct_ingest_summary")
reasons = mart("fct_dead_letter_reasons")
coherence = mart("fct_coherence")
rec = mart("rec_stream_vs_batch")
cal = mart("fct_calibration")
flagged = mart("fct_flagged_recent")
vol = mart("fct_volume_anomalies")

valid = int(summary.iloc[0]["valid_count"]) if not summary.empty else 0
dlq = int(summary.iloc[0]["dlq_count"]) if not summary.empty else 0
reject_rate = float(summary.iloc[0]["reject_rate"]) if not summary.empty else 0.0

violations = 0
if not coherence.empty and "is_incoherent" in coherence:
    violations = int(coherence["is_incoherent"].sum())

# Reconciliation is scored over a recent window only. Lifetime counts get
# polluted by orphaned old flags (stream flags whose valid counterpart was
# pruned reconcile as phantom STREAM_ONLY), which understates live agreement.
REC_WINDOW_HOURS = 48
rec_recent = rec
if not rec.empty and "minute" in rec:
    mx = pd.to_datetime(rec["minute"]).max()
    rec_recent = rec[pd.to_datetime(rec["minute"]) >= mx - pd.Timedelta(hours=REC_WINDOW_HOURS)]

agreement = None
if not rec_recent.empty and "reconciliation_status" in rec_recent:
    counts = rec_recent["reconciliation_status"].value_counts()
    total = int(counts.sum())
    both = int(counts.get("BOTH", 0))
    if total:
        agreement = both / total


# --------------------------------------------------------------------------- #
# Masthead
# --------------------------------------------------------------------------- #
st.markdown(
    f"""
    <div class="masthead">
      <div class="eyebrow">Real-time market-integrity monitoring</div>
      <h1>Prediction-Market Surveillance
        {freshness_pill()}
      </h1>
      <p>A streaming pipeline that ingests a live Polymarket feed and watches it for two things:
      whether the data itself is trustworthy (contract validation, dead-lettering, stream-vs-batch
      reconciliation) and whether the market is behaving coherently (outcome probabilities that
      fail to sum to one, a classic manipulation and mispricing signal). Raw events stay on the
      collector; only aggregated marts are published here, refreshed every 15 minutes.</p>
    </div>
    """,
    unsafe_allow_html=True,
)


# --------------------------------------------------------------------------- #
# KPI row
# --------------------------------------------------------------------------- #
def kpi(col, label, value, sub, tone=""):
    col.markdown(
        f'<div class="kpi"><div class="label">{label}</div>'
        f'<div class="value {tone}">{value}</div>'
        f'<div class="sub">{sub}</div></div>',
        unsafe_allow_html=True,
    )


k1, k2, k3, k4 = st.columns(4)
kpi(k1, "Valid events", f"{valid:,}", "contract-validated and persisted")
kpi(k2, "Contract reject rate", f"{reject_rate * 100:.1f}%",
    f"{dlq:,} dead-lettered", tone="good" if reject_rate < 0.02 else "warn")
kpi(k3, "Coherence violations", f"{violations:,}",
    "outcomes not summing to ~1", tone="good" if violations == 0 else "bad")
kpi(k4, "Stream-batch agreement",
    f"{agreement * 100:.1f}%" if agreement is not None else "—",
    f"records matched across paths &middot; last {REC_WINDOW_HOURS}h",
    tone="good" if (agreement or 0) >= 0.95 else "warn")


# --------------------------------------------------------------------------- #
# Data-health row: reject reasons + reconciliation
# --------------------------------------------------------------------------- #
left, right = st.columns(2)

with left:
    st.markdown('<div class="section-title">Why records were rejected</div>', unsafe_allow_html=True)
    st.markdown('<div class="section-note">Dead-letter reasons from the streaming contract check.</div>',
                unsafe_allow_html=True)
    if not reasons.empty:
        d = reasons.sort_values(reasons.columns[1], ascending=True)
        fig = go.Figure(go.Bar(
            x=d[d.columns[1]], y=d["reject_reason"], orientation="h",
            marker_color=AMBER, marker_line_width=0,
        ))
        st.plotly_chart(plotly_shell(fig, height=260), use_container_width=True)
    else:
        st.caption("No rejects in the current window - the live contract is holding.")

with right:
    st.markdown('<div class="section-title">Stream vs batch reconciliation</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="section-note">Did the real-time path agree with the batch re-computation? '
                f'Last {REC_WINDOW_HOURS}h.</div>',
                unsafe_allow_html=True)
    if not rec_recent.empty:
        rc = rec_recent["reconciliation_status"].value_counts().reset_index()
        rc.columns = ["status", "n"]
        palette = {"BOTH": GREEN, "STREAM_ONLY": AMBER, "BATCH_ONLY": RED}
        fig = go.Figure(go.Bar(
            x=rc["status"], y=rc["n"],
            marker_color=[palette.get(s, BLUE) for s in rc["status"]],
            marker_line_width=0,
        ))
        st.plotly_chart(plotly_shell(fig, height=260), use_container_width=True)
    else:
        st.caption("Reconciliation pending the next batch cycle.")


# --------------------------------------------------------------------------- #
# Surveillance: coherence
# --------------------------------------------------------------------------- #
st.markdown('<div class="section-title">Market coherence - the surveillance signal</div>',
            unsafe_allow_html=True)
st.markdown(
    '<div class="section-note">In a well-formed market, the outcome probabilities sum to ~1. '
    'Persistent deviation flags stale quotes, thin books, or possible manipulation.</div>',
    unsafe_allow_html=True,
)

if not coherence.empty and violations > 0:
    bad = coherence[coherence["is_incoherent"]].copy()
    if "minute" in bad and "deviation" in bad:
        fig = go.Figure(go.Scatter(
            x=bad["minute"], y=bad["deviation"], mode="markers",
            marker=dict(size=7, color=bad["deviation"].abs(), colorscale="YlOrRd",
                        showscale=False, line=dict(width=0)),
        ))
        fig.add_hline(y=0, line_color=BORDER, line_width=1)
        fig.update_yaxes(title_text="sum(outcomes) - 1")
        st.plotly_chart(plotly_shell(fig, height=280), use_container_width=True)

    show = bad.sort_values("minute", ascending=False).head(50)
    cols = [c for c in ["market_id", "minute", "outcome_sum", "deviation"] if c in show]
    st.dataframe(
        show[cols].style.format({"outcome_sum": "{:.4f}", "deviation": "{:+.4f}"})
                        .background_gradient(subset=["deviation"], cmap="RdYlGn_r"),
        use_container_width=True, hide_index=True,
    )
else:
    st.caption("No coherence violations in the current window. Markets are summing cleanly.")


# --------------------------------------------------------------------------- #
# Surveillance: the actual detections (price-range flags + volume anomalies)
# --------------------------------------------------------------------------- #
st.markdown('<div class="section-title">Recent surveillance flags</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-note">The detections themselves, not just data health: real-time '
    'price-range flags from the stream, and batch volume z-score outliers (a wash-trading footprint).</div>',
    unsafe_allow_html=True,
)

fl, vo = st.columns(2)
with fl:
    st.markdown('<div class="section-note">Price-range flags &middot; most recent</div>', unsafe_allow_html=True)
    if not flagged.empty:
        show = flagged.sort_values("window_start", ascending=False).head(50)
        cols = [c for c in ["window_start", "market_id", "price_range", "volume"] if c in show]
        st.dataframe(
            show[cols].style.format({"price_range": "{:.4f}", "volume": "{:,.0f}"}),
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("No recent price-range flags in the published window.")
with vo:
    st.markdown('<div class="section-note">Volume anomalies &middot; z-score outliers</div>', unsafe_allow_html=True)
    if not vol.empty:
        show = vol.sort_values("volume_z", ascending=False).head(50)
        cols = [c for c in ["minute", "market_id", "volume", "volume_z"] if c in show]
        st.dataframe(
            show[cols].style.format({"volume": "{:,.0f}", "volume_z": "{:+.2f}"})
                            .background_gradient(subset=["volume_z"], cmap="YlOrRd"),
            use_container_width=True, hide_index=True,
        )
    else:
        st.caption("No volume anomalies in the published window.")


# --------------------------------------------------------------------------- #
# Calibration
# --------------------------------------------------------------------------- #
st.markdown('<div class="section-title">Calibration vs real resolutions</div>', unsafe_allow_html=True)
st.markdown(
    '<div class="section-note">Once markets resolve, how close were the prices to the truth? '
    'Brier score, lower is better.</div>',
    unsafe_allow_html=True,
)

if not cal.empty and "brier_score" in cal:
    m1, _ = st.columns([1, 3])
    kpi(m1, "Mean Brier score", f"{cal['brier_score'].mean():.4f}",
        f"over {len(cal):,} resolved outcomes")
    detail_cols = [c for c in ["market_id", "outcome", "final_price", "won", "brier_score"] if c in cal]
    st.dataframe(
        cal.sort_values("brier_score", ascending=False).head(50)[detail_cols]
           .style.format({"final_price": "{:.3f}", "brier_score": "{:.4f}"}),
        use_container_width=True, hide_index=True,
    )
else:
    st.caption(
        "Awaiting first market resolution. Calibration populates only when a market this collector "
        "tracked actually resolves, so the wait depends on those markets' end dates."
    )


# --------------------------------------------------------------------------- #
# Footer
# --------------------------------------------------------------------------- #
st.markdown(
    """
    <div class="foot">
      Polymarket WebSocket -> Redpanda -> PySpark Structured Streaming -> Parquet lake -> dbt marts -> S3.
      &nbsp;&middot;&nbsp; Source: <a href="https://github.com/elaiken3/prediction-market-surveillance" target="_blank">github.com/elaiken3/prediction-market-surveillance</a>
    </div>
    """,
    unsafe_allow_html=True,
)
