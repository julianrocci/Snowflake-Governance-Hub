import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="Snowflake Cost Governance",
    page_icon="📊",
    layout="wide"
)

DB = "MGMT_DB"
SCHEMA = "COST_GOVERNANCE"

session = get_active_session()

def run_query(query):
    return session.sql(query).to_pandas()

def page_cost():
    st.title("Cost Optimization")

    tab1, tab2 = st.tabs(["Efficiency by Domain", "Wakeups & Isolated Queries"])

    with tab1:
        st.subheader("30-Day Compute Efficiency by Domain")
        efficiency = run_query(f"""
            SELECT
                DOMAIN,
                SUM(TOTAL_COMPUTE_CREDITS) AS TOTAL_COMPUTE_CREDITS,
                SUM(TOTAL_COMPUTE_CREDITS) - SUM(TOTAL_WORK_SECONDS) / 3600 AS IDLE_CREDITS,
                ROUND(
                    SUM(TOTAL_WORK_SECONDS) / NULLIF(SUM(TOTAL_BILLED_SECONDS), 0) * 100,
                    2
                ) AS EFFICIENCY_PERCENTAGE
            FROM {DB}.{SCHEMA}.RPT_WAREHOUSE_EFFICIENCY
            GROUP BY DOMAIN
            ORDER BY TOTAL_COMPUTE_CREDITS DESC
        """)

        if efficiency.empty:
            st.warning("No efficiency data available.")
        else:
            st.dataframe(efficiency, use_container_width=True)
            st.bar_chart(efficiency.set_index("DOMAIN")[["TOTAL_COMPUTE_CREDITS", "IDLE_CREDITS"]])

    with tab2:
        st.subheader("Daily Wakeup & Isolated Query Rate by Domain")
        behavior = run_query(f"""
            SELECT
                DOMAIN,
                EXECUTION_DATE::DATE AS DATE,
                SUM(TOTAL_QUERIES) AS TOTAL_QUERIES,
                SUM(WAKEUP_COUNT) AS WAKEUP_COUNT,
                SUM(ISOLATED_QUERY_COUNT) AS ISOLATED_QUERY_COUNT,
                ROUND(SUM(WAKEUP_COUNT) / NULLIF(SUM(TOTAL_QUERIES), 0) * 100, 2) AS PCT_WAKEUP,
                ROUND(SUM(ISOLATED_QUERY_COUNT) / NULLIF(SUM(TOTAL_QUERIES), 0) * 100, 2) AS PCT_ISOLATED
            FROM {DB}.{SCHEMA}.RPT_WAREHOUSE_BEHAVIOR_ANALYSIS
            GROUP BY DOMAIN, EXECUTION_DATE::DATE
            ORDER BY DATE DESC, DOMAIN
        """)

        if behavior.empty:
            st.warning("No behavior data available.")
        else:
            sort_col = st.session_state.get("beh_sort_col", "DATE")
            sort_asc = st.session_state.get("beh_sort_asc", False)

            behavior = behavior.sort_values(sort_col, ascending=sort_asc)

            cols = behavior.columns.tolist()
            st.markdown(
                "<style>.sort-row {display:flex; gap:0;} "
                ".sort-row button {font-size:12px !important; padding:4px 8px !important; "
                "border-radius:0 !important; min-height:0 !important;}</style>",
                unsafe_allow_html=True
            )
            header_cols = st.columns(len(cols))
            for i, c in enumerate(cols):
                arrow = ""
                if sort_col == c:
                    arrow = " ▲" if sort_asc else " ▼"
                with header_cols[i]:
                    if st.button(c + arrow, key=f"beh_{c}", use_container_width=True):
                        if sort_col == c:
                            st.session_state["beh_sort_asc"] = not sort_asc
                        else:
                            st.session_state["beh_sort_col"] = c
                            st.session_state["beh_sort_asc"] = False
                        st.experimental_rerun()

            st.dataframe(behavior, use_container_width=True)
            chart = behavior[["DATE", "DOMAIN", "PCT_WAKEUP", "PCT_ISOLATED"]].copy()
            chart["DATE"] = chart["DATE"].astype(str)
            st.line_chart(
                chart.set_index("DATE")[["PCT_WAKEUP", "PCT_ISOLATED"]]
            )


def page_performance():
    st.title("Performance Tuning")

    tab1, tab2, tab3, tab4 = st.tabs([
        "Cache Performance", "Spilling Summary", "Spilling Details", "Auto-Clustering"
    ])

    with tab1:
        cache = run_query(f"""
            SELECT * FROM {DB}.{SCHEMA}.RPT_WAREHOUSE_CACHE_PERFORMANCE
            ORDER BY avg_execution_time_ms DESC
        """)
        if cache.empty:
            st.info("No cache performance data.")
        else:
            col1, col2, col3 = st.columns(3)
            avg_cache = cache["PCT_RESULT_CACHE_HITS"].mean()
            avg_local = cache["PCT_LOCAL_DISK_EFFICIENT"].mean()
            avg_remote = cache["PCT_REMOTE_DISK_HEAVY"].mean()
            col1.metric("Avg Result Cache Hit %", f"{avg_cache:.1f}%")
            col2.metric("Avg Local Disk Efficient %", f"{avg_local:.1f}%")
            col3.metric("Avg Remote Heavy %", f"{avg_remote:.1f}%")
            st.dataframe(cache, use_container_width=True)

    with tab2:
        spilling = run_query(f"""
            SELECT * FROM {DB}.{SCHEMA}.RPT_WAREHOUSE_SPILLING_SUMMARY
            ORDER BY pct_critical_remote_spilling DESC
        """)
        if spilling.empty:
            st.info("No spilling data.")
        else:
            st.dataframe(spilling, use_container_width=True)

    with tab3:
        spill_detail = run_query(f"""
            SELECT * FROM {DB}.{SCHEMA}.FCT_QUERY_SPILLING_DETAILS
            ORDER BY execution_time_ms DESC
            LIMIT 50
        """)
        if spill_detail.empty:
            st.success("No problematic spilling queries detected.")
        else:
            st.warning(f"{len(spill_detail)} queries with bad spilling")
            st.dataframe(spill_detail, use_container_width=True)

    with tab4:
        clustering = run_query(f"""
            SELECT * FROM {DB}.{SCHEMA}.RPT_SERVERLESS_AUTOCLUSTERING_MART
            ORDER BY total_monthly_credits DESC
        """)
        if clustering.empty:
            st.info("No auto-clustering activity detected.")
        else:
            st.metric("Total Clustering Credits", f"{clustering['TOTAL_MONTHLY_CREDITS'].sum():,.4f}")
            st.dataframe(clustering, use_container_width=True)


def page_data_quality():
    st.title("Data Quality Monitoring")

    tab1, tab2 = st.tabs(["Freshness & Volume Gaps", "Technical Errors"])

    with tab1:
        st.subheader("Data Flow Gaps (GAP_DETECTED / SILENT_FAILURE)")
        gaps = run_query(f"""
            SELECT
                DATE_TRUNC('day', EVENT_TIMESTAMP) AS EVENT_DATE,
                EVENT_TIMESTAMP,
                FULL_TABLE_NAME,
                ALERT_LEVEL,
                TOTAL_ROWS_INSERTED,
                N_QUERIES,
                EVENT_DESCRIPTION
            FROM {DB}.{SCHEMA}.FCT_QUALITY_MONITORING
            WHERE CATEGORY = 'DATA_FLOW'
              AND ALERT_LEVEL IN ('GAP_DETECTED', 'SILENT_FAILURE')
            ORDER BY EVENT_TIMESTAMP DESC
        """)

        if gaps.empty:
            st.success("No gaps or silent failures detected.")
        else:
            col1, col2 = st.columns(2)
            gap_count = len(gaps[gaps["ALERT_LEVEL"] == "GAP_DETECTED"])
            silent_count = len(gaps[gaps["ALERT_LEVEL"] == "SILENT_FAILURE"])
            col1.metric("Gap Detected", gap_count)
            col2.metric("Silent Failures", silent_count)
            st.dataframe(gaps, use_container_width=True)

    with tab2:
        st.subheader("Automation Failures (Tasks / Pipes / Transformations)")
        errors = run_query(f"""
            SELECT
                query_id,
                start_time,
                automation_type,
                CASE
                    WHEN automation_type = 'TASK' AND query_text ILIKE '%EXECUTE TASK%'
                        THEN REGEXP_SUBSTR(query_text, 'EXECUTE\\\\s+TASK\\\\s+([\\\\w\\\\.]+)', 1, 1, 'i', 1)
                    WHEN automation_type = 'INGESTION_PIPE' AND query_text ILIKE '%COPY INTO%'
                        THEN REGEXP_SUBSTR(query_text, 'COPY\\\\s+INTO\\\\s+([\\\\w\\\\.]+)', 1, 1, 'i', 1)
                    ELSE COALESCE(NULLIF(user_name, ''), 'UNKNOWN')
                END AS object_name,
                execution_status,
                error_message,
                ROUND(duration_seconds, 1) AS duration_seconds
            FROM {DB}.{SCHEMA}.INT_AUTOMATION_ERRORS
            ORDER BY start_time DESC
        """)

        if errors.empty:
            st.success("No automation failures detected.")
        else:
            col1, col2, col3 = st.columns(3)
            tasks = len(errors[errors["AUTOMATION_TYPE"] == "TASK"])
            pipes = len(errors[errors["AUTOMATION_TYPE"] == "INGESTION_PIPE"])
            transfo = len(errors[errors["AUTOMATION_TYPE"] == "TRANSFORMATION_JOB"])
            col1.metric("Task Failures", tasks)
            col2.metric("Pipe Failures", pipes)
            col3.metric("Transformation Failures", transfo)
            st.dataframe(errors, use_container_width=True)


pages = {
    "Cost Optimization": page_cost,
    "Performance Tuning": page_performance,
    "Data Quality": page_data_quality,
}

st.sidebar.title("Snowflake Governance")
selection = st.sidebar.radio("Navigate", list(pages.keys()))
pages[selection]()