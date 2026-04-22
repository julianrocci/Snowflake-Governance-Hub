import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

session = get_active_session()

DOMAINS = {
    "FINANCE": {"prefix": "FIN", "label": "Finance"},
    "MARKETING": {"prefix": "MKT", "label": "Marketing"},
    "ECOMMERCE": {"prefix": "ECO", "label": "E-Commerce"},
    "RETAIL": {"prefix": "RET", "label": "Retail"},
    "LOYALTY": {"prefix": "LOY", "label": "Loyalty"},
    "MANAGEMENT": {"prefix": "MGMT", "label": "Management"},
}

ENVS = {"DEV": "_DEV", "UAT": "_UAT", "PROD": ""}

ALLOWED_ROLES = ("ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN")


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


@st.cache_data(ttl=60)
def get_users():
    rows = session.sql("SHOW USERS IN ACCOUNT").collect()
    return sorted([r["name"] for r in rows])


def get_user_roles(username):
    rows = session.sql(f"SHOW GRANTS TO USER {username}").collect()
    return [r["role"] for r in rows]


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


def render_domain_checkboxes(key_prefix, current_grants=None):
    selections = {}
    cols = st.columns(len(DOMAINS))
    for i, (domain, info) in enumerate(DOMAINS.items()):
        with cols[i]:
            st.markdown(f"**{info['label']}**")
            current = current_grants.get(domain, "none") if current_grants else "none"
            select_key = f"{key_prefix}_{domain}_select"
            all_key = f"{key_prefix}_{domain}_all"
            if select_key not in st.session_state:
                st.session_state[select_key] = current in ("select", "all")
            if all_key not in st.session_state:
                st.session_state[all_key] = current == "all"
            sel = st.checkbox("Select", key=select_key)
            adm = st.checkbox("All", key=all_key)
            if adm:
                selections[domain] = "all"
            elif sel:
                selections[domain] = "select"
            else:
                selections[domain] = "none"
    return selections


def page_create():
    st.header("Create Users")
    st.markdown("Enter one or more usernames separated by commas.")

    usernames_input = st.text_input("Usernames", placeholder="USER1, USER2, USER3")

    st.subheader("Environments")
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

    st.subheader("Domain Access")
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

    def render_domain_expander_multi_env(domain_key, col_container=None, default_level="select", show_roles=True):
        info = DOMAINS[domain_key]
        prefix = info["prefix"]
        container = col_container if col_container else st
        domain_selections = {}
        with container.expander(f"Access for {info['label']}", expanded=True):
            if env_order:
                env_cols = st.columns(len(env_order))
                for idx, env_name in enumerate(env_order):
                    suffix = ENVS[env_name]
                    reader_name = f"{prefix}_READER{suffix}"
                    admin_name = f"{prefix}_ADMIN{suffix}"
                    with env_cols[idx]:
                        st.markdown(f"**{env_name}**")
                        if show_roles:
                            st.caption(f"`{reader_name}`")
                            st.caption(f"`{admin_name}`")
                        else:
                            st.write("")
                            st.write("")
                        level = st.radio(
                            f"{env_name}",
                            options=["Reader", "Admin"],
                            index=0 if default_level == "select" else 1,
                            key=f"create_{domain_key}_{env_name}_level",
                            horizontal=True,
                            label_visibility="collapsed"
                        )
                        domain_selections[env_name] = "select" if level == "Reader" else "all"
            else:
                st.info("Select at least one environment above.")
        return domain_selections

    if all_domains_selected and env_order:
        bulk_level = st.radio(
            "Access for All Domains",
            options=["Reader (Select)", "Admin (Full Access)"],
            key="create_all_domains_level",
            horizontal=True
        )
        bulk = "select" if "Reader" in bulk_level else "all"

        domain_pairs = [list(DOMAINS.keys())[i:i+2] for i in range(0, len(DOMAINS), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander_multi_env(domain_key, cols[idx], default_level=bulk, show_roles=False)
                for env_name, level in domain_sels.items():
                    if env_name not in selections:
                        selections[env_name] = {}
                    selections[env_name][domain_key] = level
    elif env_order:
        domain_pairs = [active_domains[i:i+2] for i in range(0, len(active_domains), 2)]
        for pair in domain_pairs:
            cols = st.columns(2)
            for idx, domain_key in enumerate(pair):
                domain_sels = render_domain_expander_multi_env(domain_key, cols[idx])
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


def page_manage():
    st.header("Manage Existing Users")

    def reset_checkboxes():
        for key in list(st.session_state.keys()):
            if key.startswith("manage_") and (key.endswith("_select") or key.endswith("_all")):
                st.session_state[key] = False

    all_users = get_users()
    selected_user = st.selectbox("Search user", options=[""] + all_users, index=0)

    if not selected_user:
        st.info("Select a user to view and manage their access.")
        return

    st.subheader("Environment")
    env_choice = st.radio("Select environment", options=["DEV", "UAT", "PROD"], horizontal=True, key="manage_env")
    env_suffix = ENVS[env_choice]

    current_grants = get_domain_grants(selected_user, env_suffix)
    all_roles = get_user_roles(selected_user)

    st.divider()

    col_info, col_grants = st.columns([1, 2])

    with col_info:
        st.subheader("Current Roles")
        if all_roles:
            for r in sorted(all_roles):
                st.code(r)
        else:
            st.info("No roles assigned.")

    with col_grants:
        st.subheader(f"Manage Domain Access ({env_choice})")

        active_domains = [d for d, l in current_grants.items() if l != "none"]
        if active_domains:
            labels = [f"{DOMAINS[d]['label']} ({'Admin' if current_grants[d] == 'all' else 'Reader'})" for d in active_domains]
            st.caption(f"Current access: {', '.join(labels)}")
        else:
            st.caption("No domain access configured.")

        new_selections = render_domain_checkboxes(f"manage_{selected_user}_{env_choice}", current_grants)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Apply Changes", type="primary"):
                role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
                changes = {}
                for domain in DOMAINS:
                    old = current_grants.get(domain, "none")
                    new = new_selections[domain]
                    if old != new:
                        apply_domain_grants(selected_user, domain, new, env_suffix, old)
                        changes[domain] = {"from": old, "to": new, "wh_role": f"{DOMAINS[domain]['prefix']}_WH{env_suffix}_USER"}

                if changes:
                    has_any_grant = any(v != "none" for v in new_selections.values())
                    action_type = "DISABLE_USER" if not has_any_grant else "UPDATE_GRANTS"
                    domains_affected = list(changes.keys())

                    log_action(action_type, [selected_user], role_used, env_choice, domains_affected, changes,
                               f"{'Disabled' if not has_any_grant else 'Updated grants for'} {selected_user} in {env_choice}")

                    if not has_any_grant:
                        st.warning(f"{selected_user} has no more access in {env_choice} — marked as disabled.")
                    else:
                        st.success(f"Access updated for {selected_user} in {env_choice}")

                    st.experimental_rerun()
                else:
                    st.info("No changes detected.")

        with col2:
            st.button("Reset", on_click=reset_checkboxes)


st.set_page_config(page_title="User & Grants Manager", layout="wide")
st.title("User & Grants Manager")

current_role = check_access()
st.caption(f"Logged in as **{session.sql('SELECT CURRENT_USER()').collect()[0][0]}** | Role: **{current_role}**")

tab1, tab2 = st.tabs(["Create Users", "Manage Users"])

with tab1:
    page_create()
with tab2:
    page_manage()