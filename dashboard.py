"""A read-only Streamlit research dashboard over persisted PostgreSQL results.

The dashboard does not classify text, run a sequential model, or manufacture
sample data. The analytics consumer owns those operations. All timestamps shown
in the research charts are the source timestamps, in UTC.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any
from urllib.parse import parse_qs, urlparse

import altair as alt
import pandas as pd
import streamlit as st

from radar.classifier import PAIN_CATEGORIES
from radar.sequence import MIN_HISTORY

alt.data_transformers.disable_max_rows()

PALETTE = ["#6558F5", "#137C8B", "#3D69B0", "#D58B35", "#A667A7", "#4B8F79"]
ALL_RUNS = "__latest__"


@dataclass
class Snapshot:
    run: dict[str, Any]
    metrics: dict[str, Any]
    pipeline: dict[str, Any]
    windows: list[dict[str, Any]]
    distribution: list[dict[str, Any]]
    heatmap: list[dict[str, Any]]
    evidence: list[dict[str, Any]]
    signals: list[dict[str, Any]]


def safe_source_url(value: Any) -> str | None:
    """Only offer source links back to the collected HN item, never arbitrary URLs."""
    if not isinstance(value, str):
        return None
    try:
        parsed = urlparse(value)
        item = parse_qs(parsed.query).get("id", [])
        if (
            parsed.scheme == "https"
            and parsed.netloc == "news.ycombinator.com"
            and parsed.path == "/item"
            and len(item) == 1
            and item[0].isdigit()
        ):
            return f"https://news.ycombinator.com/item?id={item[0]}"
    except (TypeError, ValueError):
        pass
    return None


def utc_stamp(value: Any, *, short: bool = False) -> str:
    if value is None or pd.isna(value):
        return "—"
    stamp = pd.Timestamp(value)
    if stamp.tzinfo is None:
        stamp = stamp.tz_localize("UTC")
    else:
        stamp = stamp.tz_convert("UTC")
    return stamp.strftime("%m-%d %H:%M" if short else "%Y-%m-%d %H:%M:%S UTC")


def seconds_since(value: datetime | None, now: datetime | None = None) -> float | None:
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return max(0.0, ((now or datetime.now(timezone.utc)) - value).total_seconds())


def age_label(seconds: float | None) -> str:
    if seconds is None:
        return "No records yet"
    if seconds < 60:
        return f"{int(seconds)}s ago"
    if seconds < 3600:
        return f"{int(seconds // 60)}m ago"
    return f"{seconds / 3600:.1f}h ago"


def source_lower_bound(first: datetime, last: datetime, period: str) -> datetime:
    """Whole UTC source-hour boundaries keep metrics consistent with saved buckets."""
    first_hour = first.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    last_hour = last.astimezone(timezone.utc).replace(minute=0, second=0, microsecond=0)
    hours = {"Last 24 source hours": 24, "Last 7 source days": 168}.get(period)
    return max(first_hour, last_hour - timedelta(hours=hours - 1)) if hours else first_hour


def _connect(database_url: str):
    from radar.db import connect

    return connect(database_url)


def load_runs(database_url: str, mode: str) -> list[dict[str, Any]]:
    with _connect(database_url) as connection:
        connection.execute("SET TRANSACTION READ ONLY")
        connection.execute("SET LOCAL statement_timeout = 6000")
        return connection.execute(
            """SELECT run_id, mode, count(*) AS stored_records,
                      min(source_created_at) AS first_source,
                      max(source_created_at) AS last_source,
                      max(received_at) AS last_received
                 FROM raw_events WHERE mode = %s
                 GROUP BY run_id, mode ORDER BY last_received DESC LIMIT 40""",
            (mode,),
        ).fetchall()


def load_snapshot(database_url: str, mode: str, run_id: str, period: str,
                  evidence_scope: str = "Pain reports") -> Snapshot | None:
    with _connect(database_url) as connection:
        connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY")
        connection.execute("SET LOCAL statement_timeout = 6000")
        selected = "" if run_id == ALL_RUNS else " AND run_id = %s"
        args = (mode,) if run_id == ALL_RUNS else (mode, run_id)
        run = connection.execute(
            f"""SELECT run_id, mode, count(*) AS stored_records,
                       min(source_created_at) AS first_source,
                       max(source_created_at) AS last_source,
                       max(received_at) AS last_received
                  FROM raw_events WHERE mode = %s {selected}
                  GROUP BY run_id, mode ORDER BY last_received DESC LIMIT 1""",
            args,
        ).fetchone()
        if not run:
            return None
        since = source_lower_bound(run["first_source"], run["last_source"], period)
        run["since"] = since
        filters = (run["run_id"], mode, since)
        clause = "run_id = %s AND mode = %s AND source_created_at >= %s"
        metrics = connection.execute(
            f"""SELECT count(*) AS analyzed,
                       count(*) FILTER (WHERE is_pain) AS pain_records,
                       count(DISTINCT thread_id) FILTER (WHERE is_pain) AS pain_threads,
                       count(*) FILTER (WHERE category = 'Uncertain') AS uncertain,
                       count(*) FILTER (WHERE category = 'No pain expressed') AS neutral
                  FROM painpoint_results WHERE {clause}""",
            filters,
        ).fetchone()
        pipeline = connection.execute(
            """SELECT count(*) AS all_analyzed, max(classified_at) AS last_classified,
                      percentile_cont(0.5) WITHIN GROUP
                        (ORDER BY EXTRACT(EPOCH FROM (classified_at - emitted_at)))
                        AS median_latency_seconds
                 FROM painpoint_results WHERE run_id = %s AND mode = %s""",
            (run["run_id"], mode),
        ).fetchone()
        windows = connection.execute(
            """SELECT window_start, category, record_count, thread_count,
                      total_ai_records, pain_share
                 FROM painpoint_windows
                 WHERE run_id = %s AND mode = %s AND window_start >= %s
                 ORDER BY window_start, category""",
            filters,
        ).fetchall()
        distribution = connection.execute(
            f"""SELECT category, count(*) AS record_count,
                       count(DISTINCT thread_id) AS thread_count
                  FROM painpoint_results WHERE {clause} AND is_pain
                  GROUP BY category ORDER BY record_count DESC, category""",
            filters,
        ).fetchall()
        heatmap = connection.execute(
            f"""SELECT coalesce(nullif(product, ''), 'Unspecified') AS product,
                       category, count(*) AS record_count
                  FROM painpoint_results WHERE {clause} AND is_pain
                  GROUP BY product, category ORDER BY record_count DESC""",
            filters,
        ).fetchall()
        evidence_filter = {
            "Pain reports": " AND is_pain",
            "All classifications": "",
            "Uncertain": " AND category = 'Uncertain'",
        }.get(evidence_scope, " AND is_pain")
        evidence = connection.execute(
            f"""SELECT source_created_at, product, category, is_pain, evidence, title,
                       source_url, thread_id, method, rule_version
                  FROM painpoint_results WHERE {clause} {evidence_filter}
                  ORDER BY source_created_at DESC, event_id DESC LIMIT 500""",
            filters,
        ).fetchall()
        signals = connection.execute(
            """SELECT window_start, category, actual_count, predicted_count,
                      residual, anomaly_score, is_anomaly, history_windows, model_version
                 FROM sequence_signals WHERE run_id = %s AND window_start >= %s
                 ORDER BY window_start, category""",
            (run["run_id"], since),
        ).fetchall()
        return Snapshot(run, metrics, pipeline, windows, distribution, heatmap, evidence, signals)


def _chart_style(chart: alt.TopLevelMixin) -> alt.TopLevelMixin:
    return chart.configure_view(strokeWidth=0).configure_axis(
        labelFontSize=12, titleFontSize=12, labelColor="#52627B", titleColor="#52627B",
        gridColor="#E8ECF4", domain=False, tickColor="#E8ECF4",
        labelFont="sans-serif", titleFont="sans-serif",
    ).configure_legend(labelFontSize=12, titleFontSize=12, labelColor="#52627B",
                       labelFont="sans-serif")


def _color(legend: alt.Legend | None = None) -> alt.Color:
    return alt.Color("category:N", scale=alt.Scale(domain=list(PAIN_CATEGORIES), range=PALETTE),
                     legend=legend, title="Issue category")


def render_overview(snapshot: Snapshot) -> None:
    st.subheader("Issues over time")
    st.caption("Hourly counts by original publication time.")
    if not snapshot.windows:
        st.info("Events are arriving. Waiting for the first analyzed hour.")
        return
    windows = pd.DataFrame(snapshot.windows)
    windows["window_start"] = pd.to_datetime(windows["window_start"], utc=True)
    windows["Source hour (UTC)"] = windows["window_start"].apply(utc_stamp)
    trend = alt.Chart(windows).mark_line(point=alt.OverlayMarkDef(size=22), strokeWidth=2.5).encode(
        x=alt.X("window_start:T", title="Published (UTC)", scale=alt.Scale(type="utc"),
                axis=alt.Axis(format="%m-%d %H:%M")),
        y=alt.Y("record_count:Q", title="Reports", scale=alt.Scale(zero=True),
                axis=alt.Axis(tickMinStep=1, format="d")),
        color=_color(alt.Legend(orient="top", columns=2, title=None, columnPadding=14)),
        tooltip=[alt.Tooltip("Source hour (UTC):N"),
                 alt.Tooltip("category:N", title="Issue category"), alt.Tooltip("record_count:Q", title="Reports"),
                 alt.Tooltip("thread_count:Q", title="Threads"),
                 alt.Tooltip("pain_share:Q", title="Share of AI feedback", format=".1%")],
    ).properties(height=360)
    st.altair_chart(_chart_style(trend), width="stretch")

    left, right = st.columns([1, 1.35], gap="large")
    with left:
        st.subheader("Reports by category")
        st.caption("A thread can contain more than one report.")
        distribution = pd.DataFrame(snapshot.distribution)
        if distribution.empty:
            st.info("No pain reports in this time range yet.")
        else:
            bars = alt.Chart(distribution).mark_bar(cornerRadiusEnd=4, size=21).encode(
                y=alt.Y("category:N", sort="-x", title=None, axis=alt.Axis(labelLimit=160)),
                x=alt.X("record_count:Q", title="Reports", axis=alt.Axis(tickMinStep=1, format="d")), color=_color(),
                tooltip=[alt.Tooltip("category:N", title="Issue category"), alt.Tooltip("record_count:Q", title="Reports"),
                         alt.Tooltip("thread_count:Q", title="Threads")],
            ).properties(height=240)
            st.altair_chart(_chart_style(bars), width="stretch")
    with right:
        st.subheader("Issues by product")
        st.caption("Top eight product labels by report count.")
        heat = pd.DataFrame(snapshot.heatmap)
        if heat.empty:
            st.info("The heatmap appears after the first pain report.")
        else:
            top_products = heat.groupby("product")["record_count"].sum().nlargest(8).index.tolist()
            heat = heat[heat["product"].isin(top_products)].copy()
            maximum_count = int(heat["record_count"].max())
            legend_values = list(range(maximum_count + 1)) if maximum_count <= 5 else [0, (maximum_count + 1) // 2, maximum_count]
            plot = alt.Chart(heat).mark_rect(cornerRadius=3).encode(
                x=alt.X("category:N", sort=list(PAIN_CATEGORIES), title=None,
                        axis=alt.Axis(labelAngle=-35, labelLimit=180)),
                y=alt.Y("product:N", sort=alt.SortField("record_count", order="descending"), title=None,
                        axis=alt.Axis(labelOverlap=False, labelLimit=150, ticks=False)),
                color=alt.Color("record_count:Q", title="Reports", scale=alt.Scale(scheme="purpleblue", domain=[0, maximum_count]),
                                legend=alt.Legend(orient="bottom", gradientLength=120, values=legend_values, format="d")),
                tooltip=[alt.Tooltip("product:N", title="Product"), alt.Tooltip("category:N", title="Issue category"),
                         alt.Tooltip("record_count:Q", title="Reports")],
            ).properties(height=max(280, 34 * len(top_products) + 140))
            st.altair_chart(_chart_style(plot), width="stretch")


def render_evidence(snapshot: Snapshot) -> None:
    st.subheader("Source excerpts")
    scope = st.radio("Filter by label", ["Pain reports", "All classifications", "Uncertain"], horizontal=True, key="evidence_scope")
    st.caption("Up to 500 recent items in the selected time range.")
    if not snapshot.evidence:
        st.info("No matching records yet. Try All classifications.")
        return
    rows = pd.DataFrame(snapshot.evidence).rename(columns={"category": "Issue category"})
    if scope == "Pain reports":
        rows = rows[rows["is_pain"]]
    elif scope == "Uncertain":
        rows = rows[rows["Issue category"] == "Uncertain"]
    if rows.empty:
        st.info("No matching records. Try All classifications.")
        return
    rows["Published (UTC)"] = rows["source_created_at"].apply(utc_stamp)
    rows["Product"] = rows["product"].fillna("Unspecified")
    rows["Source link"] = rows["source_url"].apply(safe_source_url)
    rows["Excerpt"] = rows["evidence"].fillna("").astype(str).str.slice(0, 320)
    st.dataframe(
        rows[["Published (UTC)", "Issue category", "Source link", "Product", "Excerpt"]],
        hide_index=True, width="stretch", height="auto",
        column_config={"Published (UTC)": st.column_config.TextColumn(width=164),
                       "Issue category": st.column_config.TextColumn(width=153),
                       "Source link": st.column_config.LinkColumn("Source", display_text="Open HN", width=74),
                       "Product": st.column_config.TextColumn(width=116),
                       "Excerpt": st.column_config.TextColumn(width="large")},
    )
    st.caption("Open HN to read the excerpt in context.")
    with st.expander("How the labels work"):
        st.write("The analytics consumer applies one label per item using a set of rules. "
                 "The rules can miss or misread complaints. "
                 "Read the original thread to interpret a report.")
        st.write("Classification method and version")
        st.dataframe(rows[["method", "rule_version"]].drop_duplicates(), hide_index=True,
                     width="stretch")


def render_sequence(snapshot: Snapshot) -> None:
    st.subheader("Forecasts and anomalies")
    st.caption("Each forecast uses the preceding complete source hours.")
    st.info("The first and last hours are excluded. The first eight usable hours establish the baseline. "
            "Zero indicates no reports of this category in the collected sample.")
    if not snapshot.signals:
        st.write("Waiting for a complete source hour.")
        return
    signals = pd.DataFrame(snapshot.signals)
    signals["window_start"] = pd.to_datetime(signals["window_start"], utc=True)
    signals["Source hour (UTC)"] = signals["window_start"].apply(utc_stamp)
    available = [c for c in PAIN_CATEGORIES if c in set(signals["category"])]
    available.extend(c for c in sorted(set(signals["category"])) if c not in available)
    chosen = st.selectbox("Issue category", available, key="sequence_category")
    selected = signals[signals["category"] == chosen].copy()
    ready = selected[selected["history_windows"] >= MIN_HISTORY]
    a, b, c = st.columns(3)
    a.metric("Complete hours", f"{len(selected):,}", border=True)
    b.metric("Hours scored", f"{len(ready):,}", border=True)
    c.metric("Anomalous hours", f"{int(ready['is_anomaly'].sum()):,}", border=True)
    if ready.empty:
        st.warning("The model is warming up. Anomaly scoring begins after eight complete hours.")
    series = selected.melt(id_vars=["window_start", "history_windows", "Source hour (UTC)"],
                           value_vars=["actual_count", "predicted_count"],
                           var_name="series", value_name="count")
    series["Series"] = series["series"].map({"actual_count": "Observed", "predicted_count": "Forecast"})
    lines = alt.Chart(series).mark_line(strokeWidth=2.5, point=alt.OverlayMarkDef(size=20)).encode(
        x=alt.X("window_start:T", title="Complete source hour (UTC)", scale=alt.Scale(type="utc"),
                axis=alt.Axis(format="%m-%d %H:%M")),
        y=alt.Y("count:Q", title="Reports in this category", scale=alt.Scale(zero=True),
                axis=alt.Axis(tickMinStep=1)),
        color=alt.Color("Series:N", scale=alt.Scale(domain=["Observed", "Forecast"],
                                                 range=["#137C8B", "#6558F5"]), title=None,
                        legend=alt.Legend(orient="top", title=None, columns=2)),
        strokeDash=alt.StrokeDash("Series:N", scale=alt.Scale(domain=["Observed", "Forecast"],
                                                           range=[[1, 0], [5, 4]]),
                                 legend=alt.Legend(orient="top", title=None, columns=2)),
        tooltip=[alt.Tooltip("Source hour (UTC):N"),
                 "Series:N", alt.Tooltip("count:Q", title="Reports", format=".2f"),
                 alt.Tooltip("history_windows:Q", title="Prior complete hours")],
    )
    anomalies = selected[(selected["history_windows"] >= MIN_HISTORY) & selected["is_anomaly"]]
    dots = alt.Chart(anomalies).mark_point(color="#DC6469", filled=True, size=100).encode(
        x="window_start:T", y="actual_count:Q",
        tooltip=[alt.Tooltip("Source hour (UTC):N"),
                 alt.Tooltip("actual_count:Q", title="Observed"),
                 alt.Tooltip("predicted_count:Q", title="Forecast", format=".2f"),
                 alt.Tooltip("anomaly_score:Q", title="Anomaly score", format=".2f")],
    )
    st.altair_chart(_chart_style((lines + dots).properties(height=300)), width="stretch")
    st.caption("Red points indicate saved anomaly flags.")
    if not anomalies.empty:
        table = anomalies[["window_start", "actual_count", "predicted_count", "anomaly_score", "history_windows"]].copy()
        table["window_start"] = table["window_start"].apply(utc_stamp)
        table.columns = ["Source hour (UTC)", "Observed", "Forecast", "Anomaly score", "Prior hours"]
        st.dataframe(table.tail(20), hide_index=True, width="stretch")
    st.caption("Model version " + ", ".join(sorted(set(signals["model_version"].astype(str)))))


def render_definitions(snapshot: Snapshot) -> None:
    st.subheader("Data and definitions")
    st.write("Public Hacker News posts and comments are collected by keyword and filtered for AI relevance. "
             "The sample describes the discussions found by those searches.")
    st.markdown("**Live feed** polls Hacker News for new items.\n\n"
                "**Historical replay** streams public items in publication order. "
                "Each pass has its own run ID.")
    st.markdown("**Metrics** count unique items per run. A pain report is an item flagged as a concrete problem. "
                "Pain share divides reports by all analyzed items. Threads counts their distinct root posts.")
    st.markdown("**Time** in the charts refers to original publication in UTC. Ingestion and latency use pipeline time. "
                "The 24-hour filter is relative to the latest source hour in the run.")
    st.markdown("Recurring problems can help choose interview topics. "
                "Market size and willingness to pay require separate evidence.")
    st.code(f"run_id: {snapshot.run['run_id']}\nmode: {snapshot.run['mode']}\nsource: hacker_news", language=None)
    st.link_button("Hacker News API", "https://github.com/HackerNews/API")


@st.fragment(run_every="3s")
def live_panel(database_url: str, mode: str, selected_run: str, period: str) -> None:
    try:
        snapshot = load_snapshot(database_url, mode, selected_run, period,
                                 st.session_state.get("evidence_scope", "Pain reports"))
    except Exception as exc:
        # Do not print connection exceptions: some drivers include DSNs or credentials.
        st.error("Database unavailable. Retrying every 3 seconds.")
        st.caption(f"Database error type is {type(exc).__name__}.")
        return
    if snapshot is None:
        st.info("Waiting for the first events in this mode.")
        st.caption("The dashboard updates automatically as events arrive.")
        return

    m = snapshot.metrics
    analyzed = int(m.get("analyzed") or 0)
    pain = int(m.get("pain_records") or 0)
    threads = int(m.get("pain_threads") or 0)
    previous_key = f"metric_previous:{snapshot.run['run_id']}:{period}"
    previous = st.session_state.get(previous_key)
    st.session_state[previous_key] = analyzed
    delta = f"+{analyzed - previous}" if previous is not None and analyzed > previous else None
    with st.container(key="summary_metrics"):
        cols = st.columns(4)
        cols[0].metric("Items analyzed", f"{analyzed:,}", delta=delta, border=True)
        cols[1].metric("Pain reports", f"{pain:,}", border=True)
        cols[2].metric("Threads", f"{threads:,}", border=True)
        cols[3].metric("Pain share", f"{pain / analyzed:.1%}" if analyzed else "—", border=True)

    p = snapshot.pipeline
    age = seconds_since(snapshot.run["last_received"])
    latency = p.get("median_latency_seconds")
    latency_text = f"{max(0.0, float(latency)):.2f}s" if latency is not None else "Pending"
    label = "Live feed" if mode == "live" else "Historical replay"
    st.caption(
        f"{label}  ·  Last stored {age_label(age)}  ·  {snapshot.run['stored_records']:,} stored"
        f" / {int(p.get('all_analyzed') or 0):,} analyzed  ·  Median emit-to-analysis latency {latency_text}"
    )
    st.caption(
        f"Published {utc_stamp(snapshot.run['since'], short=True)} to {utc_stamp(snapshot.run['last_source'], short=True)} UTC"
        f"  ·  Run {snapshot.run['run_id']}"
    )
    if selected_run == ALL_RUNS and age is not None and age > 90:
        st.warning("No new arrivals in the last 90 seconds.")
    if analyzed == 0:
        st.info("Events are arriving. Waiting for classification.")
    overview, evidence, sequence, definitions = st.tabs(["Overview", "Source excerpts", "Forecasts", "Data"])
    with overview:
        render_overview(snapshot)
        st.caption(f"{int(m.get('neutral') or 0):,} items had no identified pain and "
                   f"{int(m.get('uncertain') or 0):,} were uncertain.")
    with evidence:
        render_evidence(snapshot)
    with sequence:
        render_sequence(snapshot)
    with definitions:
        render_definitions(snapshot)


CSS = """
<style>
  .stApp { color: #172A46; }
  .block-container { max-width: 1480px; padding-top: 1.4rem; padding-bottom: 2rem; }
  [data-testid="stSidebar"] { border-right: 1px solid #DFE5F0; }
  [data-testid="stSidebar"] .block-container { padding-top: 2rem; }
  h1, h2, h3 { letter-spacing: -0.025em; }
  h3 { font-size: 1.15rem !important; }
  p, li { font-size: 1rem; }
  [data-testid="stCaptionContainer"], .stCaption { color: #52627B !important; opacity: 1; }
  [data-testid="stCaptionContainer"] p { font-size: .875rem; color: #52627B !important; line-height: 1.6; overflow-wrap: anywhere; }
  [data-testid="stMetric"] { background: #FFFFFF; min-height: 105px; border-radius: 12px; }
  [data-testid="stMetricValue"] { font-size: 2rem; font-weight: 650; color: #172A46; }
  [data-testid="stMetricLabel"] p { font-size: .9rem; }
  .radar-hero { background: linear-gradient(120deg, #162442 0%, #24325C 100%); border-radius: 16px;
                 padding: 1.4rem 1.65rem; margin-bottom: 1.3rem; color: #FFFFFF; }
  .radar-hero h1.radar-title { color: #FFFFFF; font-size: 2rem; line-height: 1.25; font-weight: 650;
                              margin: 0 0 .45rem; padding: 0; }
  .radar-subtitle { color: #CAD4E6; font-size: .95rem; margin: 0; line-height: 1.6; }
  [data-baseweb="tab-list"] { gap: 1.4rem; border-bottom: 1px solid #DFE5F0; }
  [data-baseweb="tab"] { font-size: 1rem; padding: .6rem .05rem; }
  [data-testid="stVegaLiteChart"] { background: #FFFFFF; border: 1px solid #E5EAF3; border-radius: 12px; }
  a { text-underline-offset: 3px; }
  @media (max-width: 760px) {
    .block-container { padding: 4.5rem .8rem 2rem; }
    .radar-hero { padding: 1.2rem; }
    .radar-hero h1.radar-title { font-size: 1.6rem; }
    .st-key-summary_metrics [data-testid="stHorizontalBlock"] { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); }
    .st-key-summary_metrics [data-testid="stColumn"] { width: auto; min-width: 0; }
    .st-key-summary_metrics [data-testid="stMetric"] { min-height: 85px; }
    [data-testid="stMetricValue"] { font-size: 1.6rem; }
    [data-baseweb="tab-list"] { gap: .55rem; }
    [data-baseweb="tab"] { font-size: .87rem; }
  }
</style>
"""


def main() -> None:
    st.set_page_config(page_title="AI Pain Point Radar", page_icon="◈", layout="wide",
                       initial_sidebar_state="auto")
    st.markdown(CSS, unsafe_allow_html=True)  # Static, application-owned CSS only.
    st.markdown(
        '<section class="radar-hero"><h1 class="radar-title">AI Pain Point Radar</h1>'
        '<p class="radar-subtitle">Track recurring problems with AI tools in public Hacker News discussions.</p>'
        '</section>',
        unsafe_allow_html=True,  # No user/source text is interpolated into this static markup.
    )
    database_url = os.getenv("DATABASE_URL", "").strip()
    with st.sidebar:
        st.markdown("### Filters")
        mode_label = st.radio("Source mode", ["Historical replay", "Live feed"],
                              index=int(os.getenv("STREAM_MODE", "replay").lower() == "live"), key="mode_label")
        mode = "replay" if mode_label == "Historical replay" else "live"
        runs: list[dict[str, Any]] = []
        if database_url:
            try:
                runs = load_runs(database_url, mode)
            except Exception:
                pass  # live_panel reports a safe error and retries automatically.
        selected_run = st.selectbox("Run", [ALL_RUNS] + [row["run_id"] for row in runs],
                                    format_func=lambda value: "Follow latest run" if value == ALL_RUNS else value,
                                    key=f"run_{mode}")
        period = st.selectbox("Source time range", ["Entire run", "Last 24 source hours", "Last 7 source days"], key="period")
        st.divider()
        st.markdown("**Refreshes every 3 seconds**")
        st.caption("Charts use original publication time (UTC).")
    if not database_url:
        st.error("DATABASE_URL is missing. Start the stack using the README.")
        return
    live_panel(database_url, mode, selected_run, period)


if __name__ == "__main__":
    main()
