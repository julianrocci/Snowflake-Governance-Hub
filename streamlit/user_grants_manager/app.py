import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

session = get_active_session()

# -- Domain configuration: prefix used to build role names (e.g. FIN_READER_DEV) --
DOMAINS = {
    "FINANCE": {"prefix": "FIN", "label": "Finance"},
    "MARKETING": {"prefix": "MKT", "label": "Marketing"},
    "ECOMMERCE": {"prefix": "ECO", "label": "E-Commerce"},
    "RETAIL": {"prefix": "RET", "label": "Retail"},
    "LOYALTY": {"prefix": "LOY", "label": "Loyalty"},
    "MANAGEMENT": {"prefix": "MGMT", "label": "Management"},
}

# -- Environment suffixes appended to role/db names --
ENVS = {"DEV": "_DEV", "UAT": "_UAT", "PROD": ""}

ALLOWED_ROLES = ("ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN")


# ============================================================
# Access control & audit logging
# ============================================================

def section(title):
    st.markdown(f'<div class="section-divider"><span class="section-divider-title">{title}</span></div>', unsafe_allow_html=True)


def check_access():
    role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
    if role not in ALLOWED_ROLES:
        st.error(f"Access denied. Current role: **{role}**. Only {', '.join(ALLOWED_ROLES)} can access this application.")
        st.stop()
    return role


def log_action(action_type, target_users, role_used, env, domains_affected, grants_detail, comment=""):
    users_arr = ", ".join([f"'{u}'" for u in target_users])
    domains_arr = ", ".join([f"'{d}'" for d in domains_affected]) if domains_affected else ""
    grants_json = json.dumps(grants_detail) if grants_detail else "{}"
    session.sql(f"""
        INSERT INTO MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG 
        (ACTION_TYPE, TARGET_USERS, PERFORMED_BY, ROLE_USED, ENV, DOMAINS_AFFECTED, GRANTS_DETAIL, COMMENT)
        SELECT 
            '{action_type}',
            ARRAY_CONSTRUCT({users_arr}),
            CURRENT_USER(),
            '{role_used}',
            '{env}',
            ARRAY_CONSTRUCT({domains_arr}),
            PARSE_JSON('{grants_json}'),
            '{comment}'
    """).collect()


# ============================================================
# Data helpers
# ============================================================

@st.cache_data(ttl=60)
def get_users():
    rows = session.sql("SHOW USERS IN ACCOUNT").collect()
    return sorted([r["name"] for r in rows if r["name"] is not None])


def get_user_roles(username):
    rows = session.sql(f"SHOW GRANTS TO USER {username}").collect()
    return [r["role"] for r in rows if r["role"] is not None]


def get_domain_grants(username, env_suffix):
    user_roles = get_user_roles(username)
    grants = {}
    for domain, info in DOMAINS.items():
        prefix = info["prefix"]
        reader_role = f"{prefix}_READER{env_suffix}"
        admin_role = f"{prefix}_ADMIN{env_suffix}"
        level = "none"
        if admin_role in user_roles:
            level = "all"
        elif reader_role in user_roles:
            level = "select"
        grants[domain] = level
    return grants


# ============================================================
# Grant application logic (revoke old, grant new)
# ============================================================

def apply_domain_grants(username, domain, level, env_suffix, current_level):
    prefix = DOMAINS[domain]["prefix"]
    reader_role = f"{prefix}_READER{env_suffix}"
    admin_role = f"{prefix}_ADMIN{env_suffix}"
    analyst_role = f"{prefix}_ANALYST{env_suffix}"
    wh_role = f"{prefix}_WH{env_suffix}_USER"

    if current_level == "all" and level != "all":
        session.sql(f"REVOKE ROLE {admin_role} FROM USER {username}").collect()
    if current_level == "select" and level != "select":
        session.sql(f"REVOKE ROLE {reader_role} FROM USER {username}").collect()
    if current_level in ("all", "select") and level == "none":
        try:
            session.sql(f"REVOKE ROLE {analyst_role} FROM USER {username}").collect()
        except:
            pass
        try:
            session.sql(f"REVOKE ROLE {wh_role} FROM USER {username}").collect()
        except:
            pass

    if level == "select" and current_level != "select":
        session.sql(f"GRANT ROLE {reader_role} TO USER {username}").collect()
    elif level == "all" and current_level != "all":
        if current_level == "select":
            session.sql(f"REVOKE ROLE {reader_role} FROM USER {username}").collect()
        session.sql(f"GRANT ROLE {admin_role} TO USER {username}").collect()

    if level in ("select", "all") and current_level == "none":
        session.sql(f"GRANT ROLE {wh_role} TO USER {username}").collect()


# ============================================================
# Shared UI: domain expander with per-env role names + radio
# ============================================================

def render_domain_expander(domain_key, env_order, col_container=None, default_level="select",
                           show_roles=True, key_prefix="create", current_grants_by_env=None,
                           allow_none=False):
    info = DOMAINS[domain_key]
    prefix = info["prefix"]
    container = col_container if col_container else st
    domain_selections = {}
    options = ["None", "Reader", "Admin"] if allow_none else ["Reader", "Admin"]
    level_map = {"None": "none", "Reader": "select", "Admin": "all"}
    with container.expander(f"{info['label']}", expanded=True):
        if env_order:
            env_cols = st.columns(len(env_order))
            for idx, env_name in enumerate(env_order):
                suffix = ENVS[env_name]
                reader_name = f"{prefix}_READER{suffix}"
                admin_name = f"{prefix}_ADMIN{suffix}"
                if current_grants_by_env:
                    current = current_grants_by_env.get(env_name, {}).get(domain_key, "none")
                    if allow_none:
                        radio_index = {"none": 0, "select": 1, "all": 2}.get(current, 0)
                    else:
                        radio_index = 0 if current == "select" else 1
                else:
                    if allow_none:
                        radio_index = 0
                    else:
                        radio_index = 0 if default_level == "select" else 1
                with env_cols[idx]:
                    st.markdown(f"**{env_name}**")
                    if show_roles:
                        st.caption(f"`{reader_name}`")
                        st.caption(f"`{admin_name}`")
                    level = st.radio(
                        f"{env_name}",
                        options=options,
                        index=radio_index,
                        key=f"{key_prefix}_{domain_key}_{env_name}_level",
                        horizontal=False,
                        label_visibility="collapsed"
                    )
                    domain_selections[env_name] = level_map[level]
        else:
            st.info("Select at least one environment above.")
    return domain_selections


# ============================================================
# Page: Create Users
# ============================================================

def page_create():
    st.markdown("Enter one or more usernames separated by commas.")

    usernames_input = st.text_input("Usernames", placeholder="USER1, USER2, USER3")

    section("Environments")
    env_cols = st.columns(3)
    selected_envs = []
    with env_cols[0]:
        if st.checkbox("DEV", key="env_dev"):
            selected_envs.append("DEV")
    with env_cols[1]:
        if st.checkbox("UAT", key="env_uat"):
            selected_envs.append("UAT")
    with env_cols[2]:
        if st.checkbox("PROD", key="env_prod"):
            selected_envs.append("PROD")

    section("Domain Access")
    st.caption("*Warehouse usage role for each selected domain is automatically granted.*")
    domain_labels = ["All Domains"] + [info["label"] for info in DOMAINS.values()]
    selected_domains = st.multiselect("Select domains", options=domain_labels)

    all_domains_selected = "All Domains" in selected_domains
    if all_domains_selected:
        active_domains = list(DOMAINS.keys())
    else:
        active_domains = [k for k, v in DOMAINS.items() if v["label"] in selected_domains]

    selections = {}
    env_order = [e for e in ["DEV", "UAT", "PROD"] if e in selected_envs]

    if all_domains_selected and env_order:
        bulk_level = st.radio(
            "Access for All Domains",
            options=["Reader (Select)", "Admin (Full Access)"],
            key="create_all_domains_level",
            horizontal=True
        )
        bulk = "select" if "Reader" in bulk_level else "all"

        domain_pairs = [active_domains[i:i+2] for i in range(0, len(active_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander(domain_key, env_order, cols[idx],
                                                     default_level=bulk, show_roles=True, key_prefix="create")
                for env_name, level in domain_sels.items():
                    if env_name not in selections:
                        selections[env_name] = {}
                    selections[env_name][domain_key] = level
    elif env_order:
        domain_pairs = [active_domains[i:i+2] for i in range(0, len(active_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander(domain_key, env_order, cols[idx],
                                                     show_roles=True, key_prefix="create")
                for env_name, level in domain_sels.items():
                    if env_name not in selections:
                        selections[env_name] = {}
                    selections[env_name][domain_key] = level

    for env_name in env_order:
        if env_name not in selections:
            selections[env_name] = {}
        for domain_key in DOMAINS:
            if domain_key not in selections[env_name]:
                selections[env_name][domain_key] = "none"

    # -- Execute user creation and grant assignment --
    if st.button("Create Users", type="primary"):
        if not usernames_input.strip():
            st.error("Please enter at least one username.")
            return
        if not selected_envs:
            st.error("Please select at least one environment.")
            return

        usernames = [u.strip().upper() for u in usernames_input.split(",") if u.strip()]
        role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

        progress = st.progress(0)
        total_ops = len(usernames) * len(env_order)
        op_count = 0
        all_created = []
        all_errors = []

        for env_name in env_order:
            env_suffix = ENVS[env_name]
            env_selections = selections.get(env_name, {})
            created = []
            errors = []

            for username in usernames:
                try:
                    session.sql(f"CREATE USER IF NOT EXISTS {username} MUST_CHANGE_PASSWORD = TRUE PASSWORD = 'TempPass123!'").collect()

                    for domain, level in env_selections.items():
                        if level != "none":
                            apply_domain_grants(username, domain, level, env_suffix, "none")

                    created.append(username)
                except Exception as e:
                    errors.append(f"{username}: {str(e)}")

                op_count += 1
                progress.progress(op_count / total_ops)

            domains_affected = [d for d, l in env_selections.items() if l != "none"]
            grants_detail = {}
            for d, l in env_selections.items():
                if l != "none":
                    prefix = DOMAINS[d]["prefix"]
                    wh_role = f"{prefix}_WH{env_suffix}_USER"
                    role_name = f"{prefix}_READER{env_suffix}" if l == "select" else f"{prefix}_ADMIN{env_suffix}"
                    grants_detail[d] = {"level": l, "role": role_name, "wh_role": wh_role}

            if created:
                log_action("CREATE_USER", created, role_used, env_name, domains_affected, grants_detail,
                           f"Created {len(created)} user(s) in {env_name}")
                all_created.extend([(u, env_name) for u in created])

            all_errors.extend(errors)

        get_users.clear()

        if all_created:
            env_summary = ", ".join(env_order)
            user_list = ", ".join(sorted(set(u for u, _ in all_created)))
            st.success(f"{len(set(u for u, _ in all_created))} user(s) created in [{env_summary}]: {user_list}")

        if all_errors:
            for err in all_errors:
                st.error(err)


# ============================================================
# Page: Manage Users — shows only domains where user has access
# ============================================================

def page_manage():
    all_users = get_users()
    selected_user = st.selectbox("Search user", options=[""] + all_users, index=0)

    if not selected_user:
        return

    section("Environments")
    env_cols = st.columns(3)
    selected_envs = []
    with env_cols[0]:
        if st.checkbox("DEV", key="manage_env_dev", value=True):
            selected_envs.append("DEV")
    with env_cols[1]:
        if st.checkbox("UAT", key="manage_env_uat", value=True):
            selected_envs.append("UAT")
    with env_cols[2]:
        if st.checkbox("PROD", key="manage_env_prod", value=True):
            selected_envs.append("PROD")

    env_order = [e for e in ["DEV", "UAT", "PROD"] if e in selected_envs]

    current_grants_by_env = {}
    for env_name in env_order:
        current_grants_by_env[env_name] = get_domain_grants(selected_user, ENVS[env_name])

    active_domains = []
    for domain_key in DOMAINS:
        for env_name in env_order:
            if current_grants_by_env.get(env_name, {}).get(domain_key, "none") != "none":
                active_domains.append(domain_key)
                break

    # -- Current Roles: read-only view of existing access per domain/env --
    if active_domains and env_order:
        section("Current Roles")
        domain_pairs = [active_domains[i:i+2] for i in range(0, len(active_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                info = DOMAINS[domain_key]
                prefix = info["prefix"]
                with cols[idx]:
                    with st.expander(f"{info['label']}", expanded=True):
                        ecols = st.columns(len(env_order))
                        for eidx, env_name in enumerate(env_order):
                            suffix = ENVS[env_name]
                            current = current_grants_by_env.get(env_name, {}).get(domain_key, "none")
                            with ecols[eidx]:
                                st.markdown(f"**{env_name}**")
                                if current == "all":
                                    st.checkbox("Admin", value=True, disabled=True, key=f"cur_{selected_user}_{domain_key}_{env_name}_admin")
                                elif current == "select":
                                    st.checkbox("Reader", value=True, disabled=True, key=f"cur_{selected_user}_{domain_key}_{env_name}_reader")
                                else:
                                    st.caption("—")
    elif env_order:
        st.info(f"**{selected_user}** has no domain access in the selected environments.")

    # -- Domain Access: same UI as Create Users to add/modify grants --
    section("Domain Access")
    st.caption("*Select domains and access levels to grant.*")
    domain_labels = ["All Domains"] + [info["label"] for info in DOMAINS.values()]
    selected_domains = st.multiselect("Select domains", options=domain_labels, key="manage_domains")

    all_domains_selected = "All Domains" in selected_domains
    if all_domains_selected:
        modify_domains = list(DOMAINS.keys())
    else:
        modify_domains = [k for k, v in DOMAINS.items() if v["label"] in selected_domains]

    new_selections = {}

    if all_domains_selected and env_order:
        bulk_level = st.radio(
            "Access for All Domains",
            options=["Reader (Select)", "Admin (Full Access)"],
            key="manage_all_domains_level",
            horizontal=True
        )
        bulk = "select" if "Reader" in bulk_level else "all"

        domain_pairs = [modify_domains[i:i+2] for i in range(0, len(modify_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander(domain_key, env_order, cols[idx],
                                                     default_level=bulk, show_roles=True,
                                                     key_prefix=f"manage_{selected_user}",
                                                     current_grants_by_env=current_grants_by_env,
                                                     allow_none=True)
                for env_name, level in domain_sels.items():
                    if env_name not in new_selections:
                        new_selections[env_name] = {}
                    new_selections[env_name][domain_key] = level
    elif modify_domains and env_order:
        domain_pairs = [modify_domains[i:i+2] for i in range(0, len(modify_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander(domain_key, env_order, cols[idx],
                                                     show_roles=True,
                                                     key_prefix=f"manage_{selected_user}",
                                                     current_grants_by_env=current_grants_by_env,
                                                     allow_none=True)
                for env_name, level in domain_sels.items():
                    if env_name not in new_selections:
                        new_selections[env_name] = {}
                    new_selections[env_name][domain_key] = level

    # -- Apply changes --
    if new_selections and env_order:
        if st.button("Apply Changes", type="primary"):
            role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
            any_change = False
            for env_name in env_order:
                env_suffix = ENVS[env_name]
                current_grants = current_grants_by_env.get(env_name, get_domain_grants(selected_user, env_suffix))
                changes = {}
                for domain in modify_domains:
                    old = current_grants.get(domain, "none")
                    new = new_selections.get(env_name, {}).get(domain, old)
                    if old != new:
                        apply_domain_grants(selected_user, domain, new, env_suffix, old)
                        changes[domain] = {"from": old, "to": new, "wh_role": f"{DOMAINS[domain]['prefix']}_WH{env_suffix}_USER"}

                if changes:
                    any_change = True
                    log_action("UPDATE_GRANTS", [selected_user], role_used, env_name, list(changes.keys()), changes,
                               f"Updated grants for {selected_user} in {env_name}")

            if any_change:
                st.success(f"Access updated for {selected_user}")
                st.experimental_rerun()
            else:
                st.info("No changes detected.")


# ============================================================
# Page: Audit Trail — filterable log of all actions
# ============================================================

def page_audit():
    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        action_filter = st.selectbox("Action Type", ["All", "CREATE_USER", "UPDATE_GRANTS", "DISABLE_USER"], key="audit_action")
    with col_f2:
        env_filter = st.selectbox("Environment", ["All", "DEV", "UAT", "PROD"], key="audit_env")
    with col_f3:
        limit = st.selectbox("Show last", [25, 50, 100, 250], key="audit_limit")

    where_clauses = []
    if action_filter != "All":
        where_clauses.append(f"ACTION_TYPE = '{action_filter}'")
    if env_filter != "All":
        where_clauses.append(f"ENV = '{env_filter}'")
    where_sql = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""

    try:
        df = session.sql(f"""
            SELECT ACTION_TIMESTAMP, ACTION_TYPE, TARGET_USERS, PERFORMED_BY, ROLE_USED, ENV, DOMAINS_AFFECTED, COMMENT
            FROM MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG
            {where_sql}
            ORDER BY ACTION_TIMESTAMP DESC
            LIMIT {limit}
        """).to_pandas()

        if df.empty:
            st.info("No audit entries found.")
        else:
            stats = st.columns(4)
            with stats[0]:
                st.markdown(f'<div class="stat-card"><div class="stat-value">{len(df)}</div><div class="stat-label">Actions shown</div></div>', unsafe_allow_html=True)
            with stats[1]:
                st.markdown(f'<div class="stat-card"><div class="stat-value">{len(df[df["ACTION_TYPE"] == "CREATE_USER"])}</div><div class="stat-label">Users Created</div></div>', unsafe_allow_html=True)
            with stats[2]:
                st.markdown(f'<div class="stat-card"><div class="stat-value">{len(df[df["ACTION_TYPE"] == "UPDATE_GRANTS"])}</div><div class="stat-label">Grants Updated</div></div>', unsafe_allow_html=True)
            with stats[3]:
                st.markdown(f'<div class="stat-card"><div class="stat-value">{len(df[df["ACTION_TYPE"] == "DISABLE_USER"])}</div><div class="stat-label">Users Disabled</div></div>', unsafe_allow_html=True)
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.warning(f"Could not load audit log: {str(e)}")


# ============================================================
# Banner CSS + header
# ============================================================

st.set_page_config(page_title="User & Grants Manager", layout="wide")

st.markdown("""
<style>
    .app-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        padding: 1.5rem 2rem;
        border-radius: 12px;
        margin-bottom: 1.5rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .app-header-left { display: flex; align-items: center; gap: 1rem; }
    .app-header-icon {
        width: 48px; height: 48px;
        background: linear-gradient(135deg, #3B82F6, #8B5CF6);
        border-radius: 12px;
        display: flex; align-items: center; justify-content: center;
        font-size: 22px;
    }
    .app-header-title { color: #F8FAFC; font-size: 1.5rem; font-weight: 700; margin: 0; }
    .app-header-sub { color: #94A3B8; font-size: 0.85rem; margin: 0; }
    .header-badge {
        padding: 0.35rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
    }
    .badge-role { background: rgba(59,130,246,0.15); color: #60A5FA; border: 1px solid rgba(59,130,246,0.3); }
    .badge-user { background: rgba(139,92,246,0.15); color: #A78BFA; border: 1px solid rgba(139,92,246,0.3); }
    .stat-card {
        background: #FFFFFF;
        border: 1px solid #E2E8F0;
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
    }
    .stat-value { font-size: 2rem; font-weight: 700; color: #0F172A; }
    .stat-label { font-size: 0.8rem; color: #64748B; font-weight: 500; margin-top: 0.25rem; }

    h1 a, h2 a, h3 a, h4 a, h5 a, h6 a { display: none !important; }

    .section-divider {
        border-top: 2px solid #E2E8F0;
        padding-top: 1rem;
        margin-top: 1.5rem;
    }
    .section-divider-title {
        color: #1E293B;
        font-size: 0.85rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        margin: 0 0 0.5rem 0;
    }

    div[data-testid="stTabs"] button {
        font-size: 1.1rem !important;
        font-weight: 700 !important;
        padding: 0.75rem 2rem !important;
        letter-spacing: 0.3px;
    }
</style>
""", unsafe_allow_html=True)

current_role = check_access()
current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]

st.markdown(f"""
<div class="app-header">
    <div class="app-header-left">
        <div class="app-header-icon">&#x1f512;</div>
        <div>
            <div class="app-header-title">User & Grants Manager</div>
            <div class="app-header-sub">Identity & Access Governance Console</div>
        </div>
    </div>
    <div style="display:flex;gap:0.5rem;align-items:center;">
        <span class="header-badge badge-user">&#x1f464; {current_user}</span>
        <span class="header-badge badge-role">&#x1f511; {current_role}</span>
    </div>
</div>
""", unsafe_allow_html=True)

tab1, tab2, tab3 = st.tabs(["Create Users", "Manage Users", "Audit Trail"])

with tab1:
    page_create()
with tab2:
    page_manage()
with tab3:
    page_audit()