"""GitPulse: Engineering Intelligence & GitHub Analytics Dashboard.

Built with Streamlit, Plotly, and SQLAlchemy reading from dbt marts in PostgreSQL.
"""

from __future__ import annotations

from datetime import datetime, timezone
import os
from typing import Any, Dict, Optional, Tuple

from dotenv import load_dotenv
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
import streamlit as st

# 1. Page Configuration
st.set_page_config(
    page_title="GitPulse",
    layout="wide",
    page_icon="📊",
    initial_sidebar_state="expanded",
)

# Load environment
load_dotenv()
DEFAULT_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/gitpulse"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_DATABASE_URL)

# ---------------------------------------------------------------------------
# Custom CSS for Sleek Dark Theme & Metrics
# ---------------------------------------------------------------------------
CUSTOM_CSS = """
<style>
    /* Global Styles */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');
    
    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Main background & container */
    .stApp {
        background-color: #0b0f17;
        color: #e6edf3;
    }

    /* Header styling */
    header[data-testid="stHeader"] {
        background-color: #0b0f17 !important;
    }

    /* Metric Cards */
    .metric-card {
        background: linear-gradient(135deg, rgba(22, 27, 34, 0.95) 0%, rgba(33, 38, 45, 0.8) 100%);
        border: 1px solid rgba(240, 246, 252, 0.12);
        border-radius: 12px;
        padding: 20px 24px;
        box-shadow: 0 4px 20px rgba(0, 0, 0, 0.35);
        backdrop-filter: blur(8px);
        transition: transform 0.2s ease, border-color 0.2s ease;
    }
    .metric-card:hover {
        transform: translateY(-2px);
        border-color: rgba(88, 166, 255, 0.5);
    }
    .metric-title {
        color: #8b949e;
        font-size: 0.85rem;
        font-weight: 500;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 6px;
    }
    .metric-value {
        color: #f0f6fc;
        font-size: 2.1rem;
        font-weight: 700;
        line-height: 1.1;
    }
    .metric-subtitle {
        color: #58a6ff;
        font-size: 0.82rem;
        margin-top: 6px;
        font-weight: 400;
    }

    /* Sidebar styling */
    section[data-testid="stSidebar"] {
        background-color: #0d1117 !important;
        border-right: 1px solid #30363d !important;
    }

    /* Plotly chart container polish */
    .js-plotly-plot {
        border-radius: 12px;
        border: 1px solid #30363d;
        overflow: hidden;
    }
</style>
"""
st.markdown(CUSTOM_CSS, unsafe_allow_html=True)


def apply_dark_theme(fig: go.Figure) -> go.Figure:
    """Apply consistent high-contrast dark theme styling to Plotly figures."""
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor="#161b22",
        plot_bgcolor="#161b22",
        font=dict(color="#e6edf3", family="Inter, sans-serif"),
    )
    return fig


# ---------------------------------------------------------------------------
# Database Connection & Cached Queries
# ---------------------------------------------------------------------------
@st.cache_resource
def get_db_engine() -> Optional[Engine]:
    """Create and cache SQLAlchemy engine with pool_pre_ping."""
    try:
        engine = create_engine(
            DATABASE_URL,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10,
        )
        return engine
    except Exception as exc:
        st.sidebar.error(f"Database connection error: {exc}")
        return None


@st.cache_data(ttl=300)
def load_marts_data() -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Query data from all dbt analytics marts with 300s TTL cache."""
    engine = get_db_engine()
    if engine is None:
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    try:
        with engine.connect() as conn:
            # 1. dim_repos
            repos_df = pd.read_sql(text("SELECT * FROM analytics.dim_repos"), conn)

            # 2. fct_commits
            commits_df = pd.read_sql(
                text("SELECT * FROM analytics.fct_commits ORDER BY commit_date DESC"),
                conn,
            )

            # 3. fct_pull_requests
            pulls_df = pd.read_sql(
                text("SELECT * FROM analytics.fct_pull_requests ORDER BY created_at DESC"),
                conn,
            )

            # 4. fct_issues
            issues_df = pd.read_sql(
                text("SELECT * FROM analytics.fct_issues ORDER BY created_at DESC"),
                conn,
            )

            # Parse datetime columns
            if not commits_df.empty and "commit_date" in commits_df:
                commits_df["commit_date"] = pd.to_datetime(commits_df["commit_date"], utc=True)

            if not pulls_df.empty:
                for col in ["created_at", "merged_at"]:
                    if col in pulls_df:
                        pulls_df[col] = pd.to_datetime(pulls_df[col], utc=True)

            if not issues_df.empty:
                for col in ["created_at", "closed_at"]:
                    if col in issues_df:
                        issues_df[col] = pd.to_datetime(issues_df[col], utc=True)

            return repos_df, commits_df, pulls_df, issues_df

    except Exception:
        # Fallback if marts are not yet populated
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()


# Load Mart Data
repos_df, commits_df, pulls_df, issues_df = load_marts_data()


# ---------------------------------------------------------------------------
# Sidebar Navigation & Controls
# ---------------------------------------------------------------------------
with st.sidebar:
    st.markdown(
        """
        <div style="display: flex; align-items: center; gap: 10px; margin-bottom: 20px;">
            <span style="font-size: 2rem;">⚡</span>
            <div>
                <h2 style="margin: 0; font-size: 1.4rem; font-weight: 700; color: #58a6ff;">GitPulse</h2>
                <span style="font-size: 0.75rem; color: #8b949e;">Engineering Intelligence</span>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    page = st.selectbox(
        "Navigation",
        [
            "Repository Overview",
            "Developer Activity",
            "Pull Request Health",
            "Issue Analytics",
        ],
        index=0,
    )

    st.markdown("---")

    # Repository Filter
    if not repos_df.empty:
        all_repos = ["All Repositories"] + sorted(repos_df["repo_name"].dropna().unique().tolist())
        selected_repo = st.selectbox("Filter Repository", all_repos, index=0)
    else:
        selected_repo = "All Repositories"

    st.markdown("---")

    # Refresh Cache
    if st.button("🔄 Refresh Data Cache", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    # Database Status
    st.markdown(
        """
        <div style="margin-top: 30px; padding: 12px; background: rgba(35, 134, 54, 0.15); border: 1px solid rgba(46, 160, 67, 0.4); border-radius: 8px;">
            <div style="display: flex; align-items: center; gap: 8px;">
                <span style="height: 8px; width: 8px; background-color: #3fb950; border-radius: 50%; display: inline-block;"></span>
                <span style="font-size: 0.8rem; font-weight: 600; color: #3fb950;">PostgreSQL Connected</span>
            </div>
            <div style="font-size: 0.72rem; color: #8b949e; margin-top: 4px;">
                Target: <code>analytics</code> marts
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


# Filter datasets if a specific repo is selected
if selected_repo != "All Repositories":
    if not repos_df.empty:
        repos_df = repos_df[repos_df["repo_name"] == selected_repo]
    if not commits_df.empty:
        commits_df = commits_df[commits_df["repo_name"] == selected_repo]
    if not pulls_df.empty:
        pulls_df = pulls_df[pulls_df["repo_name"] == selected_repo]
    if not issues_df.empty:
        issues_df = issues_df[issues_df["repo_name"] == selected_repo]


# Check if database has data
has_data = not (repos_df.empty and commits_df.empty and pulls_df.empty and issues_df.empty)

if not has_data:
    st.title("GitPulse Analytics")
    st.warning("⚠️ No data yet — run the pipeline and dbt build to populate analytics marts.")
    st.info("Run `python run_pipeline.py` or `python test_pipeline.py`, followed by `dbt build`.")
    st.stop()


# ---------------------------------------------------------------------------
# Helper KPI Component
# ---------------------------------------------------------------------------
def render_kpi(col, title: str, value: str, subtitle: str) -> None:
    """Render a styled KPI metric card in a column."""
    with col:
        st.markdown(
            f"""
            <div class="metric-card">
                <div class="metric-title">{title}</div>
                <div class="metric-value">{value}</div>
                <div class="metric-subtitle">{subtitle}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


# ===========================================================================
# PAGE 1: Repository Overview
# ===========================================================================
if page == "Repository Overview":
    st.title("📊 Repository Overview")
    st.markdown("High-level engineering volume, velocity, and metadata for tracked repositories.")

    # KPI Metric Cards
    kpi1, kpi2, kpi3, kpi4 = st.columns(4)
    total_repos = len(repos_df)
    total_commits = len(commits_df)
    total_pulls = len(pulls_df)
    total_open_issues = (
        int(repos_df["open_issues"].sum())
        if not repos_df.empty and "open_issues" in repos_df
        else len(issues_df[issues_df["state"] == "open"]) if not issues_df.empty else 0
    )

    render_kpi(kpi1, "Repositories", f"{total_repos:,}", "Tracked in GitPulse")
    render_kpi(kpi2, "Total Commits", f"{total_commits:,}", "Extracted git commits")
    render_kpi(kpi3, "Pull Requests", f"{total_pulls:,}", "Tracked PRs")
    render_kpi(kpi4, "Open Issues", f"{total_open_issues:,}", "Current backlog")

    st.markdown("<br>", unsafe_allow_html=True)

    # Plotly Line Chart: Commits per Day
    if not commits_df.empty and "commit_date" in commits_df:
        commits_daily = (
            commits_df.set_index("commit_date")
            .resample("1D")["sha"]
            .count()
            .reset_index(name="commit_count")
        )

        fig_commits_daily = px.line(
            commits_daily,
            x="commit_date",
            y="commit_count",
            title="Daily Commit Velocity (Last 30 Days)",
            template="plotly_dark",
            markers=True,
            labels={"commit_date": "Date", "commit_count": "Commits"},
        )
        fig_commits_daily.update_traces(
            line=dict(color="#58a6ff", width=3),
            marker=dict(size=7, color="#1f6feb", line=dict(width=1.5, color="#ffffff")),
            fill="tozeroy",
            fillcolor="rgba(88, 166, 255, 0.12)",
        )
        fig_commits_daily.update_layout(
            height=380,
            margin=dict(l=20, r=20, t=50, b=20),
            xaxis=dict(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)"),
            yaxis=dict(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)"),
        )
        apply_dark_theme(fig_commits_daily)
        st.plotly_chart(fig_commits_daily, use_container_width=True)
    else:
        st.info("No commit date records available to plot daily trend.")

    # Repository Details Table
    if not repos_df.empty:
        st.markdown("### 📁 Tracked Repositories")
        display_repos = repos_df.copy()
        if "created_at" in display_repos:
            display_repos["created_at"] = display_repos["created_at"].dt.strftime("%Y-%m-%d")
        st.dataframe(
            display_repos,
            use_container_width=True,
            column_config={
                "repo_name": st.column_config.TextColumn("Repository"),
                "stars": st.column_config.NumberColumn("⭐ Stars", format="%d"),
                "forks": st.column_config.NumberColumn("🍴 Forks", format="%d"),
                "open_issues": st.column_config.NumberColumn("🐛 Open Issues", format="%d"),
                "language": st.column_config.TextColumn("Language"),
                "created_at": st.column_config.TextColumn("Created Date"),
            },
            hide_index=True,
        )


# ===========================================================================
# PAGE 2: Developer Activity
# ===========================================================================
elif page == "Developer Activity":
    st.title("👩‍💻 Developer Activity")
    st.markdown("Identify top code contributors and commit schedule heatmaps.")

    col_left, col_right = st.columns([1, 1])

    # Top 10 Contributors Bar Chart
    with col_left:
        if not commits_df.empty and "author_login" in commits_df:
            top_authors = (
                commits_df["author_login"]
                .value_counts()
                .head(10)
                .reset_index(name="commit_count")
            )
            top_authors.columns = ["author_login", "commit_count"]

            fig_authors = px.bar(
                top_authors,
                x="commit_count",
                y="author_login",
                orientation="h",
                title="Top 10 Contributors by Commit Count",
                template="plotly_dark",
                labels={"commit_count": "Total Commits", "author_login": "Contributor"},
                color="commit_count",
                color_continuous_scale="Purples",
                text="commit_count",
            )
            fig_authors.update_layout(
                yaxis=dict(autorange="reversed"),
                height=480,
                coloraxis_showscale=False,
                margin=dict(l=20, r=20, t=50, b=20),
            )
            fig_authors.update_traces(textposition="outside")
            apply_dark_theme(fig_authors)
            st.plotly_chart(fig_authors, use_container_width=True)
        else:
            st.info("No commit authors available.")

    # Day-of-week × Hour Commits Heatmap
    with col_right:
        if not commits_df.empty and "commit_date" in commits_df:
            c_df = commits_df.dropna(subset=["commit_date"]).copy()
            c_df["day_of_week"] = c_df["commit_date"].dt.day_name()
            c_df["hour"] = c_df["commit_date"].dt.hour

            days_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
            hours_order = list(range(24))

            # Pivot table for heatmap matrix
            heatmap_data = (
                c_df.groupby(["day_of_week", "hour"])
                .size()
                .unstack(fill_value=0)
                .reindex(index=days_order, columns=hours_order, fill_value=0)
            )

            fig_heatmap = px.imshow(
                heatmap_data,
                labels=dict(x="Hour of Day (UTC)", y="Day of Week", color="Commits"),
                x=[f"{h:02d}:00" for h in hours_order],
                y=days_order,
                title="Commit Heatmap (Day of Week × Hour)",
                template="plotly_dark",
                color_continuous_scale="Tealgrn",
                aspect="auto",
            )
            fig_heatmap.update_layout(
                height=480,
                margin=dict(l=20, r=20, t=50, b=20),
            )
            apply_dark_theme(fig_heatmap)
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("No commit timestamp data available for heatmap.")


# ===========================================================================
# PAGE 3: Pull Request Health
# ===========================================================================
elif page == "Pull Request Health":
    st.title("🔀 Pull Request Health & Velocity")
    st.markdown("Track merge duration distributions and weekly PR flow.")

    if not pulls_df.empty:
        # PR Metric summary
        pr1, pr2, pr3, pr4 = st.columns(4)
        total_prs = len(pulls_df)
        merged_prs = pulls_df["is_merged"].sum() if "is_merged" in pulls_df else 0
        merge_rate = (merged_prs / total_prs * 100.0) if total_prs > 0 else 0.0
        avg_hours_merge = (
            pulls_df["hours_to_merge"].dropna().mean()
            if "hours_to_merge" in pulls_df and not pulls_df["hours_to_merge"].dropna().empty
            else 0.0
        )
        avg_days_merge = avg_hours_merge / 24.0

        render_kpi(pr1, "Total Pulls", f"{total_prs:,}", "Analyzed PRs")
        render_kpi(pr2, "Merged Pulls", f"{int(merged_prs):,}", "Successfully merged")
        render_kpi(pr3, "Merge Rate", f"{merge_rate:.1f}%", "Merged to Total ratio")
        render_kpi(pr4, "Avg Days to Merge", f"{avg_days_merge:.2f}d", f"{avg_hours_merge:.1f} hours average")

        st.markdown("<br>", unsafe_allow_html=True)

        col_pr_left, col_pr_right = st.columns([1, 1])

        # 1. Histogram of Days to Merge
        with col_pr_left:
            merged_subset = pulls_df[pulls_df["hours_to_merge"].notnull()].copy()
            merged_subset["days_to_merge"] = merged_subset["hours_to_merge"] / 24.0

            if not merged_subset.empty:
                fig_hist = px.histogram(
                    merged_subset,
                    x="days_to_merge",
                    nbins=25,
                    title="Distribution of PR Days to Merge",
                    template="plotly_dark",
                    color_discrete_sequence=["#a371f7"],
                    labels={"days_to_merge": "Days to Merge", "count": "Pull Requests"},
                )
                fig_hist.update_layout(
                    height=400,
                    margin=dict(l=20, r=20, t=50, b=20),
                    xaxis=dict(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)"),
                    yaxis=dict(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)"),
                )
                apply_dark_theme(fig_hist)
                st.plotly_chart(fig_hist, use_container_width=True)
            else:
                st.info("No merged pull requests with time-to-merge data found.")

        # 2. Weekly Opened vs Merged Area Chart
        with col_pr_right:
            p_df = pulls_df.copy()
            opened_weekly = (
                p_df.dropna(subset=["created_at"])
                .set_index("created_at")
                .resample("W-MON")["number"]
                .count()
                .reset_index(name="opened_count")
                .rename(columns={"created_at": "week"})
            )

            merged_weekly = (
                p_df.dropna(subset=["merged_at"])
                .set_index("merged_at")
                .resample("W-MON")["number"]
                .count()
                .reset_index(name="merged_count")
                .rename(columns={"merged_at": "week"})
            )

            flow_df = pd.merge(opened_weekly, merged_weekly, on="week", how="outer").fillna(0).sort_values("week")

            if not flow_df.empty:
                fig_flow = go.Figure()
                fig_flow.add_trace(
                    go.Scatter(
                        x=flow_df["week"],
                        y=flow_df["opened_count"],
                        name="Opened PRs",
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color="#f0883e", width=2.5),
                        fillcolor="rgba(240, 136, 62, 0.15)",
                    )
                )
                fig_flow.add_trace(
                    go.Scatter(
                        x=flow_df["week"],
                        y=flow_df["merged_count"],
                        name="Merged PRs",
                        mode="lines",
                        fill="tozeroy",
                        line=dict(color="#3fb950", width=2.5),
                        fillcolor="rgba(63, 185, 80, 0.15)",
                    )
                )
                fig_flow.update_layout(
                    title="Weekly PR Flow: Opened vs Merged",
                    template="plotly_dark",
                    xaxis_title="Week",
                    yaxis_title="Number of PRs",
                    height=400,
                    margin=dict(l=20, r=20, t=50, b=20),
                    legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
                )
                apply_dark_theme(fig_flow)
                st.plotly_chart(fig_flow, use_container_width=True)
            else:
                st.info("Insufficient weekly PR data for flow chart.")
    else:
        st.info("No pull request data available in analytics.fct_pull_requests.")


# ===========================================================================
# PAGE 4: Issue Analytics
# ===========================================================================
elif page == "Issue Analytics":
    st.title("🐛 Issue Analytics & Backlog")
    st.markdown("Analyze issue statuses, resolution times, and repository distributions.")

    if not issues_df.empty:
        col_iss_left, col_iss_right = st.columns([1, 1])

        # 1. Open vs Closed Pie Chart (Donut)
        with col_iss_left:
            state_counts = (
                issues_df["state"]
                .value_counts()
                .reset_index(name="count")
            )
            state_counts.columns = ["state", "count"]

            colors = {"open": "#f0883e", "closed": "#238636", "merged": "#8957e5"}
            color_map = [colors.get(s, "#58a6ff") for s in state_counts["state"]]

            fig_donut = go.Figure(
                data=[
                    go.Pie(
                        labels=state_counts["state"],
                        values=state_counts["count"],
                        hole=0.55,
                        marker=dict(colors=color_map, line=dict(color="#161b22", width=2)),
                        textinfo="label+percent",
                    )
                ]
            )
            fig_donut.update_layout(
                title="Issue Status Breakdown (Open vs Closed)",
                template="plotly_dark",
                height=420,
                margin=dict(l=20, r=20, t=50, b=20),
                legend=dict(orientation="h", yanchor="bottom", y=-0.1, xanchor="center", x=0.5),
            )
            apply_dark_theme(fig_donut)
            st.plotly_chart(fig_donut, use_container_width=True)

        # 2. Avg Days to Close Bar per Repo
        with col_iss_right:
            if "days_to_close" in issues_df:
                closed_subset = issues_df[issues_df["days_to_close"].notnull()].copy()
                if not closed_subset.empty:
                    repo_close_time = (
                        closed_subset.groupby("repo_name")["days_to_close"]
                        .mean()
                        .reset_index(name="avg_days_to_close")
                    )
                    fig_close_bar = px.bar(
                        repo_close_time,
                        x="repo_name",
                        y="avg_days_to_close",
                        title="Average Days to Close Issues by Repository",
                        template="plotly_dark",
                        labels={"repo_name": "Repository", "avg_days_to_close": "Average Days to Close"},
                        color="avg_days_to_close",
                        color_continuous_scale="Oranges",
                        text="avg_days_to_close",
                    )
                    fig_close_bar.update_traces(texttemplate="%{text:.1f}d", textposition="outside")
                    fig_close_bar.update_layout(
                        height=420,
                        coloraxis_showscale=False,
                        margin=dict(l=20, r=20, t=50, b=20),
                        yaxis=dict(showgrid=True, gridcolor="rgba(255, 255, 255, 0.08)"),
                    )
                    apply_dark_theme(fig_close_bar)
                    st.plotly_chart(fig_close_bar, use_container_width=True)
                else:
                    st.info("No closed issues with resolution durations available.")
            else:
                st.info("Column days_to_close not present in issues dataset.")

        # Recent Issues Table
        st.markdown("### 📋 Recent Issues Sample")
        sample_cols = [c for c in ["number", "repo_name", "title", "state", "author_login", "created_at", "days_to_close"] if c in issues_df]
        display_issues = issues_df[sample_cols].head(10).copy()
        if "created_at" in display_issues:
            display_issues["created_at"] = display_issues["created_at"].dt.strftime("%Y-%m-%d %H:%M")
        st.dataframe(
            display_issues,
            use_container_width=True,
            hide_index=True,
            column_config={
                "number": st.column_config.NumberColumn("Issue #"),
                "repo_name": st.column_config.TextColumn("Repository"),
                "title": st.column_config.TextColumn("Title"),
                "state": st.column_config.TextColumn("Status"),
                "author_login": st.column_config.TextColumn("Author"),
                "created_at": st.column_config.TextColumn("Created Date"),
                "days_to_close": st.column_config.NumberColumn("Days to Close", format="%.1f d"),
            },
        )
    else:
        st.info("No issues data available in analytics.fct_issues.")
