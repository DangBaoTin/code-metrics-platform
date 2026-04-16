import os
import time

import pandas as pd
import plotly.express as px
import pymongo
import streamlit as st
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy


st.set_page_config(
    page_title="Code Metrics Dashboard",
    layout="wide",
    initial_sidebar_state="expanded",
)


def humanize_time(dt):
    """Convert timestamps to relative text."""
    if pd.isna(dt):
        return "Unknown"
    now = pd.Timestamp.utcnow().replace(tzinfo=None)
    dt_naive = pd.to_datetime(dt).replace(tzinfo=None)
    diff = now - dt_naive
    seconds = diff.total_seconds()

    if seconds < 60:
        return "Just now"
    if seconds < 3600:
        return f"{int(seconds // 60)} mins ago"
    if seconds < 86400:
        return f"{int(seconds // 3600)} hours ago"
    return f"{int(seconds // 86400)} days ago"


@st.cache_resource
def init_cassandra():
    cass_host = os.getenv("CASSANDRA_HOST", "127.0.0.1")
    cass_port = int(os.getenv("CASSANDRA_PORT", "9042"))
    contact_points = [
        host.strip()
        for host in os.getenv("CASSANDRA_CONTACT_POINTS", cass_host).split(",")
        if host.strip()
    ]
    allowed_hosts = [
        host.strip()
        for host in os.getenv("CASSANDRA_ALLOWED_HOSTS", cass_host).split(",")
        if host.strip()
    ]
    max_attempts = int(os.getenv("DASHBOARD_CASSANDRA_CONNECT_RETRIES", "6"))
    delay_seconds = float(os.getenv("DASHBOARD_CASSANDRA_RETRY_DELAY_SEC", "2"))
    connect_timeout = float(os.getenv("DASHBOARD_CASSANDRA_CONNECT_TIMEOUT_SEC", "15"))

    profile = ExecutionProfile(
        load_balancing_policy=WhiteListRoundRobinPolicy(allowed_hosts),
        request_timeout=connect_timeout,
    )

    last_error = None
    for attempt in range(1, max_attempts + 1):
        cluster = None
        try:
            cluster = Cluster(
                contact_points,
                port=cass_port,
                execution_profiles={EXEC_PROFILE_DEFAULT: profile},
                connect_timeout=connect_timeout,
                control_connection_timeout=connect_timeout,
            )
            session = cluster.connect("code_metrics")
            session.execute("SELECT release_version FROM system.local")
            return session
        except Exception as exc:
            last_error = exc
            if cluster is not None:
                cluster.shutdown()
            if attempt < max_attempts:
                time.sleep(delay_seconds)

    raise RuntimeError(
        f"Cassandra connection failed after {max_attempts} attempts: {last_error}"
    )


@st.cache_resource
def init_mongo():
    mongo_uri = os.getenv(
        "MONGO_URI",
        "mongodb://localhost:27017,localhost:27018,localhost:27019/code_metrics?replicaSet=rs0",
    )
    # Host-side runs may fail replica-set DNS discovery when members are advertised as container names.
    try:
        client = pymongo.MongoClient(mongo_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client["code_metrics"]
    except Exception:
        fallback_uri = "mongodb://localhost:27017/?directConnection=true"
        client = pymongo.MongoClient(fallback_uri, serverSelectionTimeoutMS=3000)
        client.admin.command("ping")
        return client["code_metrics"]


def apply_styles(layout_mode="Demo"):
    dense_mode = layout_mode == "Analysis"
    card_padding = "12px 12px 10px 12px" if dense_mode else "16px 16px 14px 16px"
    section_gap = "0.2rem" if dense_mode else "0.35rem"
    dataframe_min_h = "260px" if dense_mode else "300px"
    css = """
        <style>
        @import url('https://fonts.googleapis.com/css2?family=Space+Grotesk:wght@400;500;600;700&family=IBM+Plex+Mono:wght@400;500&display=swap');
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        /* Keep Streamlit header/toolbar visible so the native sidebar toggle arrow remains available. */
        .stApp {
            background: radial-gradient(circle at 20% 0%, #101c33 0%, #070c16 45%, #05080f 100%);
            color: #f4f7fb;
            font-family: 'Space Grotesk', 'Segoe UI', sans-serif;
        }
        section[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0b1220 0%, #070d18 100%);
            border-right: 1px solid rgba(105, 138, 194, 0.2);
        }
        section[data-testid="stSidebar"] * {
            font-family: 'Space Grotesk', 'Segoe UI', sans-serif;
        }
        .metric-card {
            border: 1px solid rgba(76, 112, 169, 0.35);
            background: linear-gradient(160deg, rgba(20, 34, 60, 0.9) 0%, rgba(10, 17, 31, 0.95) 100%);
            border-radius: 16px;
            padding: __CARD_PADDING__;
            min-height: 98px;
            box-shadow: 0 10px 30px rgba(0, 0, 0, 0.32);
            animation: riseIn 460ms ease-out both;
        }
        .metric-label {
            color: #9eb2d8;
            font-size: 0.8rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
        }
        .metric-value {
            font-size: 1.95rem;
            font-weight: 700;
            line-height: 1.1;
            color: #eef4ff;
        }
        .metric-chip {
            display: inline-block;
            margin-top: 8px;
            padding: 2px 10px;
            border-radius: 999px;
            font-size: 0.76rem;
            font-weight: 600;
            font-family: 'IBM Plex Mono', monospace;
        }
        .chip-green {
            background: rgba(16, 185, 129, 0.16);
            color: #55e1b0;
            border: 1px solid rgba(16, 185, 129, 0.35);
        }
        .chip-red {
            background: rgba(239, 68, 68, 0.14);
            color: #ff8f8f;
            border: 1px solid rgba(239, 68, 68, 0.35);
        }
        .section-title {
            margin-top: 0.4rem;
            margin-bottom: 0.2rem;
            font-size: 2.35rem;
            font-weight: 700;
            color: #f4f8ff;
            letter-spacing: -0.02em;
        }
        .subtle {
            color: #9aaed1;
            font-size: 0.98rem;
        }
        .section-caption {
            color: #79a3d8;
            font-size: 0.78rem;
            letter-spacing: 0.12em;
            text-transform: uppercase;
            margin-top: 1.2rem;
            margin-bottom: __SECTION_GAP__;
            font-family: 'IBM Plex Mono', monospace;
        }
        .section-divider {
            margin: 0.35rem 0 0.8rem 0;
            border: 0;
            border-top: 1px solid rgba(106, 134, 180, 0.22);
        }
        .glass-panel {
            border: 1px solid rgba(86, 126, 187, 0.28);
            background: linear-gradient(165deg, rgba(19, 31, 55, 0.75) 0%, rgba(8, 14, 26, 0.88) 100%);
            border-radius: 14px;
            padding: 10px 12px;
            margin-bottom: 0.8rem;
            animation: fadeIn 520ms ease-out both;
        }
        .status-tape {
            margin-top: 0.8rem;
            margin-bottom: 1rem;
            border: 1px solid rgba(86, 126, 187, 0.3);
            border-radius: 999px;
            background: linear-gradient(90deg, rgba(8, 15, 30, 0.95), rgba(16, 28, 52, 0.9), rgba(8, 15, 30, 0.95));
            overflow: hidden;
            white-space: nowrap;
            position: relative;
        }
        .status-tape::before {
            content: "";
            position: absolute;
            top: 0;
            left: -40%;
            width: 35%;
            height: 100%;
            background: linear-gradient(90deg, rgba(255,255,255,0), rgba(255,255,255,0.08), rgba(255,255,255,0));
            animation: tapeShimmer 3.2s linear infinite;
        }
        .status-tape-inner {
            display: inline-block;
            padding: 10px 0;
            animation: tapeScroll 24s linear infinite;
            font-family: 'IBM Plex Mono', monospace;
            color: #b8d0f0;
            font-size: 0.82rem;
            letter-spacing: 0.03em;
        }
        .status-pill {
            margin: 0 14px;
            padding: 4px 10px;
            border-radius: 999px;
            border: 1px solid rgba(115, 146, 196, 0.4);
            background: rgba(24, 42, 74, 0.55);
        }
        div[data-testid="stPlotlyChart"] {
            border: 1px solid rgba(86, 126, 187, 0.18);
            border-radius: 14px;
            padding: 6px;
            background: linear-gradient(165deg, rgba(13, 22, 38, 0.65), rgba(8, 14, 24, 0.85));
            animation: riseIn 420ms ease-out both;
        }
        div[data-testid="stDataFrame"] {
            border: 1px solid rgba(86, 126, 187, 0.18);
            border-radius: 14px;
            overflow: hidden;
            animation: fadeIn 460ms ease-out both;
            min-height: __DATAFRAME_MIN_H__;
        }
        .delay-1 { animation-delay: 60ms; }
        .delay-2 { animation-delay: 120ms; }
        .delay-3 { animation-delay: 180ms; }
        .delay-4 { animation-delay: 240ms; }
        @keyframes riseIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        @keyframes fadeIn {
            from { opacity: 0; }
            to { opacity: 1; }
        }
        @keyframes tapeScroll {
            from { transform: translateX(0); }
            to { transform: translateX(-50%); }
        }
        @keyframes tapeShimmer {
            from { left: -40%; }
            to { left: 110%; }
        }
        </style>
        """
    css = css.replace("__CARD_PADDING__", card_padding)
    css = css.replace("__SECTION_GAP__", section_gap)
    css = css.replace("__DATAFRAME_MIN_H__", dataframe_min_h)
    st.markdown(css, unsafe_allow_html=True)


def section_header(eyebrow, title, subtitle):
    st.markdown(f"<div class='section-caption'>{eyebrow}</div>", unsafe_allow_html=True)
    st.markdown(f"### {title}")
    st.markdown(f"<div class='subtle'>{subtitle}</div>", unsafe_allow_html=True)
    st.markdown("<hr class='section-divider' />", unsafe_allow_html=True)


def status_tape(stats):
    items = [
        f"System: {stats['system_status']}",
        f"Users: {stats['active_users']}",
        f"Solved: {stats['total_solved']}",
        f"Avg fail-rate: {stats['avg_fail_rate']}%",
        f"High-risk users: {stats['high_risk']}",
        f"Updated: {stats['updated_at']}",
    ]
    row = "".join([f"<span class='status-pill'>{x}</span>" for x in items])
    st.markdown(
        (
            "<div class='status-tape'><div class='status-tape-inner'>"
            f"{row}{row}"
            "</div></div>"
        ),
        unsafe_allow_html=True,
    )


def safe_cassandra_query(query: str):
    if not cass_session:
        return []
    max_attempts = int(os.getenv("DASHBOARD_CASSANDRA_QUERY_RETRIES", "3"))
    delay_seconds = float(os.getenv("DASHBOARD_CASSANDRA_QUERY_RETRY_DELAY_SEC", "0.8"))
    last_error = None
    for attempt in range(1, max_attempts + 1):
        try:
            return list(cass_session.execute(query))
        except Exception as exc:
            last_error = exc
            if attempt < max_attempts:
                time.sleep(delay_seconds)
    raise RuntimeError(last_error)


def get_quick_stats():
    baseline = {
        "system_status": "ONLINE",
        "active_users": 0,
        "total_solved": 0,
        "avg_fail_rate": 0.0,
        "high_risk": 0,
        "updated_at": pd.Timestamp.now().strftime("%H:%M:%S"),
    }
    if not cass_session:
        return baseline

    try:
        lb_rows = safe_cassandra_query("SELECT user_id, total_solved FROM gold_live_leaderboard")
        lb_df = pd.DataFrame(lb_rows)
        if not lb_df.empty:
            baseline["active_users"] = int(lb_df["user_id"].nunique())
            baseline["total_solved"] = int(lb_df["total_solved"].sum())

        hm_rows = safe_cassandra_query("SELECT fail_rate_pct FROM gold_difficulty_heatmap")
        hm_df = pd.DataFrame(hm_rows)
        if not hm_df.empty:
            baseline["avg_fail_rate"] = round(float(hm_df["fail_rate_pct"].mean()), 2)

        risk_rows = safe_cassandra_query("SELECT engagement_score FROM gold_engagement_scores")
        risk_df = pd.DataFrame(risk_rows)
        if not risk_df.empty:
            baseline["high_risk"] = int((risk_df["engagement_score"] < 100).sum())
    except Exception:
        baseline["system_status"] = "DEGRADED"

    return baseline


try:
    cass_session = init_cassandra()
    mongo_db = init_mongo()
    db_status = "Connected"
except Exception as e:
    cass_session = None
    mongo_db = None
    db_status = f"Connection failed: {e}"


with st.sidebar:
    st.caption("Use the arrow at top-left to collapse/expand sidebar")
    st.markdown("## Code Metrics")
    st.caption("Enterprise Monitoring Console")
    layout_mode = st.radio("Layout Mode", ["Demo", "Analysis"], horizontal=True, index=0)
    view = st.radio(
        "Navigation",
        [
            "Overview",
            "Live Leaderboard",
            "Difficulty Heatmap",
            "Subscription Dashboard",
            "Instructor Report Card",
            "Dropout Risk",
            "AI Recommender",
            "System Alerts",
        ],
        index=0,
        label_visibility="collapsed",
    )
    st.markdown("---")
    refresh_enabled = st.checkbox("Auto refresh", value=True)
    refresh_seconds = st.slider("Refresh interval (sec)", 2, 20, 5, 1)
    st.caption(f"DB status: {db_status}")
    st.caption(f"Last refresh: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")


apply_styles(layout_mode)


st.markdown("<div class='section-title'>Code Metrics Platform</div>", unsafe_allow_html=True)
st.markdown("<div class='subtle'>Polyglot data platform with Kafka, Spark, Cassandra, MongoDB</div>", unsafe_allow_html=True)
status_tape(get_quick_stats())


k1, k2, k3, k4 = st.columns(4)
with k1:
    st.markdown(
        """
        <div class="metric-card delay-1">
            <div class="metric-label">Pipeline Status</div>
            <div class="metric-value">HEALTHY</div>
            <span class="metric-chip chip-green">Streaming active</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with k2:
    st.markdown(
        """
        <div class="metric-card delay-2">
            <div class="metric-label">Ingestion Engine</div>
            <div class="metric-value">Kafka + Spark</div>
            <span class="metric-chip chip-red">&lt; 500ms latency</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with k3:
    st.markdown(
        """
        <div class="metric-card delay-3">
            <div class="metric-label">Telemetry Storage</div>
            <div class="metric-value">Cassandra</div>
            <span class="metric-chip chip-green">Wide-column OLAP</span>
        </div>
        """,
        unsafe_allow_html=True,
    )
with k4:
    st.markdown(
        """
        <div class="metric-card delay-4">
            <div class="metric-label">Metadata Storage</div>
            <div class="metric-value">MongoDB</div>
            <span class="metric-chip chip-green">Document OLTP</span>
        </div>
        """,
        unsafe_allow_html=True,
    )

st.markdown("\n")

if not cass_session:
    st.error(f"Database unavailable. {db_status}")
    st.stop()


def load_leaderboard():
    cass_rows = safe_cassandra_query(
        "SELECT user_id, total_solved, recent_status, last_updated FROM gold_live_leaderboard"
    )
    df_leaderboard = pd.DataFrame(cass_rows)
    if df_leaderboard.empty:
        return df_leaderboard

    users_meta = list(mongo_db["users"].find({}, {"_id": 1, "full_name": 1, "class_cohort": 1}))
    df_users = pd.DataFrame(users_meta).rename(columns={"_id": "user_id"})
    df_final = pd.merge(df_leaderboard, df_users, on="user_id", how="left")
    return df_final.sort_values(by="total_solved", ascending=False).reset_index(drop=True)


def load_heatmap():
    rows = safe_cassandra_query(
        "SELECT problem_id, category, total_attempts, failed_attempts, fail_rate_pct FROM gold_difficulty_heatmap"
    )
    df_heatmap = pd.DataFrame(rows)
    if df_heatmap.empty:
        return df_heatmap

    probs_meta = list(mongo_db["problems"].find({}, {"_id": 1, "title": 1}))
    df_probs = pd.DataFrame(probs_meta).rename(columns={"_id": "problem_id"})
    df_heatmap = pd.merge(df_heatmap, df_probs, on="problem_id", how="left")
    return df_heatmap.sort_values(by="fail_rate_pct", ascending=False).reset_index(drop=True)


def load_risk():
    rows = safe_cassandra_query(
        "SELECT user_id, dropout_probability, risk_label, last_updated FROM gold_dropout_predictions"
    )
    df_risk = pd.DataFrame(rows)
    if df_risk.empty:
        return df_risk

    tiers = safe_cassandra_query(
        "SELECT user_id, performance_tier, recommended_difficulty FROM gold_adaptive_difficulty_profiles"
    )
    df_tiers = pd.DataFrame(tiers)

    users_meta = list(mongo_db["users"].find({}, {"_id": 1, "full_name": 1, "class_cohort": 1}))
    df_users = pd.DataFrame(users_meta).rename(columns={"_id": "user_id"})
    df_risk = pd.merge(df_risk, df_users, on="user_id", how="left")
    if not df_tiers.empty:
        df_risk = pd.merge(df_risk, df_tiers, on="user_id", how="left")
    if "performance_tier" not in df_risk.columns:
        df_risk["performance_tier"] = "Unknown"
    if "recommended_difficulty" not in df_risk.columns:
        df_risk["recommended_difficulty"] = "Unknown"

    df_risk["dropout_probability"] = (df_risk["dropout_probability"] * 100).round(2)
    return df_risk.sort_values(by="dropout_probability", ascending=False)


def load_system_alerts():
    rows = safe_cassandra_query(
        "SELECT alert_id, alert_type, user_id, description, triggered_at FROM gold_system_alerts"
    )
    df_alerts = pd.DataFrame(rows)
    if df_alerts.empty:
        return df_alerts

    try:
        users_meta = list(mongo_db["users"].find({}, {"_id": 1, "full_name": 1, "class_cohort": 1}))
        df_users = pd.DataFrame(users_meta).rename(columns={"_id": "user_id"})
        df_alerts = pd.merge(df_alerts, df_users, on="user_id", how="left")
    except Exception:
        # Keep alerts usable even if metadata lookup is temporarily unavailable.
        df_alerts["full_name"] = df_alerts.get("user_id")
        df_alerts["class_cohort"] = "Unknown"
    return df_alerts.sort_values(by="triggered_at", ascending=False).reset_index(drop=True)


def load_subscription_revenue():
    rows = safe_cassandra_query(
        "SELECT plan_type, total_subscriptions, total_revenue_usd, last_updated FROM gold_subscription_revenue"
    )
    df_sub = pd.DataFrame(rows)
    if df_sub.empty:
        return df_sub
    return df_sub.sort_values(by="total_revenue_usd", ascending=False).reset_index(drop=True)


def load_instructor_report():
    rows = safe_cassandra_query(
        "SELECT instructor_id, instructor_name, avg_rating, total_ratings, last_updated FROM gold_instructor_report_card"
    )
    df_report = pd.DataFrame(rows)
    if df_report.empty:
        return df_report
    return df_report.sort_values(by="avg_rating", ascending=False).reset_index(drop=True)


def load_recommendations():
    rec_rows = safe_cassandra_query(
        "SELECT user_id, recommended_problem_id, recommendation_score, last_updated FROM gold_next_problem_recommendations"
    )
    df_rec = pd.DataFrame(rec_rows)
    if df_rec.empty:
        return df_rec

    users_meta = list(mongo_db["users"].find({}, {"_id": 1, "full_name": 1, "class_cohort": 1}))
    problems_meta = list(mongo_db["problems"].find({}, {"_id": 1, "title": 1, "difficulty": 1}))
    tiers = safe_cassandra_query(
        "SELECT user_id, performance_tier, recommended_difficulty FROM gold_adaptive_difficulty_profiles"
    )

    df_users = pd.DataFrame(users_meta).rename(columns={"_id": "user_id"})
    df_probs = pd.DataFrame(problems_meta).rename(columns={"_id": "recommended_problem_id"})
    df_tiers = pd.DataFrame(tiers)

    df_rec = pd.merge(df_rec, df_users, on="user_id", how="left")
    df_rec = pd.merge(df_rec, df_probs, on="recommended_problem_id", how="left")
    if not df_tiers.empty:
        df_rec = pd.merge(df_rec, df_tiers, on="user_id", how="left")
    if "performance_tier" not in df_rec.columns:
        df_rec["performance_tier"] = "Unknown"
    if "recommended_difficulty" not in df_rec.columns:
        df_rec["recommended_difficulty"] = "Unknown"

    return df_rec.sort_values(by="recommendation_score", ascending=False).reset_index(drop=True)


if view in ["Overview", "Live Leaderboard"]:
    section_header("STREAMING", "Live Leaderboard", "Real-time solved-volume ranking from telemetry stream")
    try:
        df_final = load_leaderboard()
        if df_final.empty:
            st.info("Waiting for telemetry data from Kafka.")
        else:
            kpi_a, kpi_b, kpi_c = st.columns(3)
            with kpi_a:
                st.markdown(
                    f"<div class='glass-panel'><b>Top Student</b><br>{df_final.iloc[0]['full_name']}</div>",
                    unsafe_allow_html=True,
                )
            with kpi_b:
                st.markdown(
                    f"<div class='glass-panel'><b>Total Active Users</b><br>{df_final['user_id'].nunique()}</div>",
                    unsafe_allow_html=True,
                )
            with kpi_c:
                st.markdown(
                    f"<div class='glass-panel'><b>Total Solved</b><br>{int(df_final['total_solved'].sum())}</div>",
                    unsafe_allow_html=True,
                )

            top10 = df_final.head(10)
            fig_top = px.bar(
                top10,
                x="full_name",
                y="total_solved",
                color="total_solved",
                color_continuous_scale=["#26b5d6", "#27e17f"],
                template="plotly_dark",
                labels={"full_name": "Student", "total_solved": "Solved"},
                title="Top 10 Students by Solved Problems",
            )
            fig_top.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=50, b=20),
                xaxis_title=None,
                yaxis_title="Solved",
            )
            fig_top.update_traces(marker_line_width=0)
            st.plotly_chart(fig_top, width="stretch")

            df_display = df_final[
                ["full_name", "class_cohort", "total_solved", "recent_status", "last_updated"]
            ].copy()
            df_display["last_updated"] = df_display["last_updated"].apply(humanize_time)
            st.dataframe(df_display, width="stretch", height=360)
    except Exception as e:
        st.error(f"Error fetching leaderboard: {e}")


if view in ["Overview", "Difficulty Heatmap"]:
    section_header("BATCH INSIGHT", "Difficulty Heatmap", "Problem and category-level failure concentration")
    try:
        df_heatmap = load_heatmap()
        if df_heatmap.empty:
            st.warning("No batch data found. Run the Airflow ETL job.")
        else:
            c1, c2 = st.columns([1.5, 1])
            with c1:
                tree = px.treemap(
                    df_heatmap,
                    path=["category", "title"],
                    values="failed_attempts",
                    color="fail_rate_pct",
                    color_continuous_scale=["#25c787", "#f4c742", "#f87171"],
                    template="plotly_dark",
                    title="Failure Volume by Category and Problem",
                )
                tree.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=45, b=10),
                    coloraxis_colorbar=dict(title="Fail %"),
                )
                st.plotly_chart(tree, width="stretch")

            with c2:
                donut = px.pie(
                    df_heatmap,
                    values="failed_attempts",
                    names="category",
                    hole=0.55,
                    template="plotly_dark",
                    color_discrete_sequence=px.colors.qualitative.Safe,
                    title="Failure Share by Category",
                )
                donut.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=45, b=10),
                )
                st.plotly_chart(donut, width="stretch")

                st.dataframe(
                    df_heatmap[["title", "category", "fail_rate_pct", "total_attempts"]].head(12),
                    width="stretch",
                    height=300,
                    column_config={
                        "fail_rate_pct": st.column_config.ProgressColumn(
                            "Fail Rate",
                            min_value=0,
                            max_value=100,
                            format="%.2f%%",
                        )
                    },
                )
    except Exception as e:
        st.error(f"Error fetching heatmap data: {e}")


if view in ["Overview", "Subscription Dashboard"]:
    section_header(
        "BUSINESS",
        "Subscription Revenue",
        "Plan-wise subscription counts and revenue aggregation",
    )
    try:
        df_sub = load_subscription_revenue()
        if df_sub.empty:
            st.info("Run nightly batch ETL to generate subscription revenue aggregates.")
        else:
            total_revenue = float(df_sub["total_revenue_usd"].sum())
            total_subs = int(df_sub["total_subscriptions"].sum())
            k1, k2 = st.columns(2)
            with k1:
                st.markdown(
                    f"<div class='glass-panel'><b>Total Revenue (USD)</b><br>${total_revenue:,.2f}</div>",
                    unsafe_allow_html=True,
                )
            with k2:
                st.markdown(
                    f"<div class='glass-panel'><b>Total Subscriptions</b><br>{total_subs}</div>",
                    unsafe_allow_html=True,
                )

            fig_sub = px.bar(
                df_sub,
                x="plan_type",
                y="total_revenue_usd",
                color="total_subscriptions",
                template="plotly_dark",
                title="Revenue by Plan",
            )
            fig_sub.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=50, b=20),
            )
            st.plotly_chart(fig_sub, width="stretch")

            st.dataframe(
                df_sub[["plan_type", "total_subscriptions", "total_revenue_usd", "last_updated"]],
                width="stretch",
                height=260,
            )
    except Exception as e:
        st.error(f"Error fetching subscription analytics: {e}")


if view in ["Overview", "Instructor Report Card"]:
    section_header(
        "ACADEMICS",
        "Instructor Report Card",
        "Spark-aggregated instructor quality scores from ratings",
    )
    try:
        df_instructor = load_instructor_report()
        if df_instructor.empty:
            st.info("Run nightly batch ETL to generate instructor scorecards.")
        else:
            fig_inst = px.bar(
                df_instructor,
                x="instructor_name",
                y="avg_rating",
                color="total_ratings",
                template="plotly_dark",
                title="Instructor Average Ratings",
                labels={"avg_rating": "Average Rating"},
            )
            fig_inst.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=50, b=20),
                xaxis_title=None,
            )
            st.plotly_chart(fig_inst, width="stretch")

            st.dataframe(
                df_instructor[
                    ["instructor_name", "avg_rating", "total_ratings", "last_updated"]
                ],
                width="stretch",
                height=280,
            )
    except Exception as e:
        st.error(f"Error fetching instructor report: {e}")


if view in ["Overview", "Dropout Risk"]:
    section_header("RISK", "Dropout Risk Monitor", "Logistic Regression dropout probability by student")
    try:
        df_risk = load_risk()
        if df_risk.empty:
            st.info("Run the nightly batch job to generate model predictions.")
        else:
            risk_colors = {"HIGH": "#ef4444", "MEDIUM": "#f59e0b", "LOW": "#22c55e"}
            fig_risk = px.scatter(
                df_risk,
                x="full_name",
                y="dropout_probability",
                color="risk_label",
                color_discrete_map=risk_colors,
                size="dropout_probability",
                size_max=20,
                template="plotly_dark",
                title="Student Dropout Probability Distribution",
                labels={"full_name": "Student", "dropout_probability": "Dropout Probability (%)"},
            )
            fig_risk.update_layout(
                plot_bgcolor="rgba(0,0,0,0)",
                paper_bgcolor="rgba(0,0,0,0)",
                margin=dict(l=20, r=20, t=50, b=20),
                xaxis_title=None,
            )
            st.plotly_chart(fig_risk, width="stretch")

            st.dataframe(
                df_risk[
                    [
                        "full_name",
                        "class_cohort",
                        "dropout_probability",
                        "risk_label",
                        "performance_tier",
                        "recommended_difficulty",
                    ]
                ],
                width="stretch",
                height=300,
                column_config={
                    "risk_label": st.column_config.TextColumn("Risk", help="Derived from model output")
                },
            )
    except Exception as e:
        st.error(f"Error fetching risk data: {e}")


if view in ["Overview", "AI Recommender"]:
    section_header(
        "AI",
        "Next-Problem Recommender",
        "Collaborative filtering recommendations and adaptive difficulty guidance",
    )
    try:
        df_rec = load_recommendations()
        if df_rec.empty:
            st.info("Run nightly batch ETL to generate recommendation outputs.")
        else:
            tier_dist = (
                df_rec["performance_tier"].value_counts().rename_axis("tier").reset_index(name="count")
                if "performance_tier" in df_rec
                else pd.DataFrame()
            )
            if not tier_dist.empty:
                fig_tier = px.pie(
                    tier_dist,
                    names="tier",
                    values="count",
                    hole=0.45,
                    template="plotly_dark",
                    title="Adaptive Tier Distribution",
                )
                fig_tier.update_layout(
                    plot_bgcolor="rgba(0,0,0,0)",
                    paper_bgcolor="rgba(0,0,0,0)",
                    margin=dict(l=10, r=10, t=45, b=10),
                )
                st.plotly_chart(fig_tier, width="stretch")

            st.dataframe(
                df_rec[
                    [
                        "full_name",
                        "class_cohort",
                        "title",
                        "difficulty",
                        "recommendation_score",
                        "performance_tier",
                        "recommended_difficulty",
                    ]
                ].head(100),
                width="stretch",
                height=320,
            )
    except Exception as e:
        st.error(f"Error fetching recommendation output: {e}")


if view in ["Overview", "System Alerts"]:
    section_header("OPERATIONS", "System Alerts", "Latency and suspicious-activity alerts from system_metrics stream")
    try:
        df_alerts = load_system_alerts()
        if df_alerts.empty:
            st.info("No alerts yet. Start simulator + streaming job to populate system alerts.")
        else:
            alert_counts = df_alerts["alert_type"].value_counts().to_dict()
            a1, a2 = st.columns(2)
            with a1:
                st.markdown(
                    f"<div class='glass-panel'><b>HIGH_LATENCY</b><br>{alert_counts.get('HIGH_LATENCY', 0)}</div>",
                    unsafe_allow_html=True,
                )
            with a2:
                st.markdown(
                    f"<div class='glass-panel'><b>CHEAT_DETECTED</b><br>{alert_counts.get('CHEAT_DETECTED', 0)}</div>",
                    unsafe_allow_html=True,
                )

            st.dataframe(
                df_alerts[["triggered_at", "alert_type", "full_name", "class_cohort", "description"]].head(50),
                width="stretch",
                height=320,
            )
    except Exception as e:
        st.error(f"Error fetching system alerts: {e}")


if refresh_enabled:
    time.sleep(refresh_seconds)
    st.rerun()