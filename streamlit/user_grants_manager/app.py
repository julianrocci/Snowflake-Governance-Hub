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
    "SALES": {"prefix": "SAL", "label": "Sales"},
    "HR": {"prefix": "HR", "label": "HR"},
}

ENVS = {"DEV": "_DEV", "UAT": "_UAT", "PROD": ""}

ALLOWED_ROLES = ("ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN")


def section(title):
    st.markdown(f'<div class="section-divider"><span class="section-divider-title">{title}</span></div>', unsafe_allow_html=True)


def check_access():
    role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
    if role not in ALLOWED_ROLES:
        st.error(f"Access denied. Current role: **{role}**. Only {', '.join(ALLOWED_ROLES)} can access this application.")
        st.stop()
    return role


# Logs one row per user per domain into USER_ACTIVITY_LOG.
# Also grants the WH usage role when a user goes from "none" to an active level.
def log_action(action_type, target_users, role_used, env, domains_affected, grants_detail, comment=""):
    if not domains_affected:
        domains_affected = ["N/A"]
    for user in target_users:
        for domain in domains_affected:
            detail = grants_detail.get(domain, {}) if grants_detail else {}
            detail_json = json.dumps(detail) if detail else "{}"
            prefix = DOMAINS[domain]["prefix"] if domain in DOMAINS else domain
            effective_comment = comment if comment.strip() else f"Created user in {prefix} {env}"
            safe_comment = effective_comment.replace("'", "''")
            role_assigned = detail.get("role", "")
            wh_role = detail.get("wh_role", "")
            old_level = detail.get("from", "none")
            new_level = detail.get("to", "")
            objects = []
            objects.append(("DATABASE", role_assigned))
            if old_level == "none" and new_level in ("select", "all"):
                objects.append(("WAREHOUSE", wh_role))
            for obj_type, role_name in objects:
                if not role_name:
                    continue
                session.sql(f"""
                    INSERT INTO MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG 
                    (ACTION_TYPE, USER_NAME, PERFORMED_BY, ROLE_USED, ENV, DOMAIN, GRANTS_DETAIL, COMMENT, ROLE_ASSIGNED, OBJECT_TYPE)
                    SELECT 
                        '{action_type}',
                        '{user}',
                        CURRENT_USER(),
                        '{role_used}',
                        '{env}',
                        '{domain}',
                        PARSE_JSON('{detail_json}'),
                        '{safe_comment}',
                        '{role_name}',
                        '{obj_type}'
                """).collect()


@st.cache_data(ttl=60)
def get_users():
    rows = session.sql("SHOW USERS IN ACCOUNT").collect()
    return sorted([r["name"] for r in rows if r["name"] is not None])


def get_user_roles(username):
    rows = session.sql(f"SHOW GRANTS TO USER {username}").collect()
    return [r["role"] for r in rows if r["role"] is not None]


# Returns {domain_key: "none"|"select"|"all"} by checking which _READER/_ADMIN roles the user holds.
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


# Revoke old level, grant new level, and manage warehouse usage role transitions.
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


# Interactive expander with per-env radio buttons (Reader/Admin/None).
# When current_grants_by_env is provided, pre-selects the user's current level.
def render_domain_expander(domain_key, env_order, col_container=None, default_level="select",
                           show_roles=True, key_prefix="create", current_grants_by_env=None,
                           allow_none=False):
    info = DOMAINS[domain_key]
    prefix = info["prefix"]
    container = col_container if col_container else st
    domain_selections = {}
    options = ["None", "Reader", "Admin"] if allow_none else ["Reader", "Admin"]
    level_map = {"None": "none", "Reader": "select", "Admin": "all"}
    with container.expander(info['label'], expanded=True):
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


# Read-only expander grid showing existing grants as disabled checkboxes.
def render_readonly_domains(active_domains, env_order, grants_by_env, key_prefix, show_role_names=True):
    domain_pairs = [active_domains[i:i+2] for i in range(0, len(active_domains), 2)]
    for pair in domain_pairs:
        cols = st.columns(2)
        for idx, domain_key in enumerate(pair):
            info = DOMAINS[domain_key]
            prefix = info["prefix"]
            with cols[idx]:
                with st.expander(info['label'], expanded=True):
                    ecols = st.columns(len(env_order))
                    for eidx, env_name in enumerate(env_order):
                        suffix = ENVS[env_name]
                        current = grants_by_env.get(env_name, {}).get(domain_key, "none")
                        with ecols[eidx]:
                            st.markdown(f"**{env_name}**")
                            if show_role_names:
                                st.caption(f"`{prefix}_READER{suffix}`")
                                st.caption(f"`{prefix}_ADMIN{suffix}`")
                            if current == "all":
                                st.checkbox("Admin", value=True, disabled=True, key=f"{key_prefix}_{domain_key}_{env_name}_admin")
                            elif current == "select":
                                st.checkbox("Reader", value=True, disabled=True, key=f"{key_prefix}_{domain_key}_{env_name}_reader")
                            else:
                                st.caption("\u2014")


# Renders domain expanders in a 2-column grid and collects selections[env][domain] = level.
def render_domain_grid(domains, env_order, selections, default_level="select",
                       key_prefix="create", current_grants_by_env=None, allow_none=False):
    domain_pairs = [domains[i:i+2] for i in range(0, len(domains), 2)]
    for pair in domain_pairs:
        cols = st.columns(2)
        for idx, domain_key in enumerate(pair):
            domain_sels = render_domain_expander(domain_key, env_order, cols[idx],
                                                 default_level=default_level, show_roles=True,
                                                 key_prefix=key_prefix,
                                                 current_grants_by_env=current_grants_by_env,
                                                 allow_none=allow_none)
            for env_name, level in domain_sels.items():
                if env_name not in selections:
                    selections[env_name] = {}
                selections[env_name][domain_key] = level


def page_create():
    st.markdown('<div class="section-divider-title">Mode</div>', unsafe_allow_html=True)
    mode = st.radio("Mode", options=["New User", "Clone from User"], horizontal=True, key="create_mode", label_visibility="collapsed")

    section("New User")
    usernames_input = st.text_input("Enter one or more usernames separated by commas.", placeholder="USER1, USER2, USER3")

    if mode == "Clone from User":
        section("Clone From")
        all_users = get_users()
        source_user = st.selectbox("Select user to clone", options=[""] + all_users, index=0, key="clone_source")

        if not source_user:
            return

        st.caption(f"New user(s) will receive the same domain access as **{source_user}** across all environments.")

        selections = {}
        env_order = list(ENVS.keys())
        for env_name in env_order:
            env_suffix = ENVS[env_name]
            grants = get_domain_grants(source_user, env_suffix)
            selections[env_name] = grants

        active_envs = []
        for env_name in env_order:
            if any(v != "none" for v in selections[env_name].values()):
                active_envs.append(env_name)

        if not active_envs:
            st.warning(f"{source_user} has no domain access to clone.")
            return

        section("Domain Access from Clone")
        active_domains = []
        for domain_key in DOMAINS:
            for env_name in active_envs:
                if selections[env_name].get(domain_key, "none") != "none":
                    active_domains.append(domain_key)
                    break

        current_grants_by_env = {env_name: selections[env_name] for env_name in active_envs}
        render_readonly_domains(active_domains, active_envs, current_grants_by_env, "clone")

        user_comment = st.text_input("Comment (optional)", key="clone_comment")
        if st.button("Clone User(s)", type="primary"):
            if not usernames_input.strip():
                st.error("Please enter at least one username.")
                return

            usernames = [u.strip().upper() for u in usernames_input.split(",") if u.strip()]
            role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

            all_created = []
            all_errors = []

            for env_name in active_envs:
                env_suffix = ENVS[env_name]
                env_grants = {d: l for d, l in selections[env_name].items() if l != "none"}
                created = []
                errors = []

                for username in usernames:
                    try:
                        session.sql(f"CREATE USER IF NOT EXISTS {username} MUST_CHANGE_PASSWORD = TRUE PASSWORD = 'TempPass123!'").collect()
                        for domain, level in env_grants.items():
                            apply_domain_grants(username, domain, level, env_suffix, "none")
                        created.append(username)
                    except Exception as e:
                        errors.append(f"{username}: {str(e)}")

                grants_detail = {}
                for d, l in env_grants.items():
                    prefix = DOMAINS[d]["prefix"]
                    grants_detail[d] = {"level": l, "role": f"{prefix}_READER{env_suffix}" if l == "select" else f"{prefix}_ADMIN{env_suffix}", "wh_role": f"{prefix}_WH{env_suffix}_USER"}

                if created:
                    comment_text = user_comment
                    log_action("CREATE_USER", created, role_used, env_name, list(env_grants.keys()), grants_detail, comment_text)
                    all_created.extend([(u, env_name) for u in created])
                all_errors.extend(errors)

            get_users.clear()
            if all_created:
                user_list = ", ".join(sorted(set(u for u, _ in all_created)))
                st.success(f"{len(set(u for u, _ in all_created))} user(s) cloned from {source_user}: {user_list}")
            for err in all_errors:
                st.error(err)
        return

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

        render_domain_grid(active_domains, env_order, selections, default_level=bulk, key_prefix="create")
    elif env_order:
        render_domain_grid(active_domains, env_order, selections, key_prefix="create")

    for env_name in env_order:
        if env_name not in selections:
            selections[env_name] = {}
        for domain_key in DOMAINS:
            if domain_key not in selections[env_name]:
                selections[env_name][domain_key] = "none"

    user_comment = st.text_input("Comment (optional)", key="create_comment")
    if st.button("Create User(s)", type="primary"):
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
                comment_text = user_comment
                log_action("CREATE_USER", created, role_used, env_name, domains_affected, grants_detail,
                           comment_text)
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
    all_users = get_users()
    selected_user = st.selectbox("Search user", options=[""] + all_users, index=0)

    if not selected_user:
        return

    section("User Info")
    try:
        user_info = session.sql(f"SHOW USERS LIKE '{selected_user}'").collect()
        if user_info:
            u = user_info[0]
            created_str = str(u["created_on"] or "—")[:16]
            last_login_raw = u["last_success_login"]
            last_login_str = str(last_login_raw)[:16] if last_login_raw else "Never"
            user_roles = get_user_roles(selected_user)

            col1, col2, col3 = st.columns(3)
            with col1:
                st.markdown(f"""
                <div class="info-card">
                    <div class="info-card-label">Login Name</div>
                    <div class="info-card-value">{u["login_name"] or "—"}</div>
                </div>
                <div class="info-card">
                    <div class="info-card-label">Email</div>
                    <div class="info-card-value">{u["email"] or "—"}</div>
                </div>
                """, unsafe_allow_html=True)
            with col2:
                st.markdown(f"""
                <div class="info-card">
                    <div class="info-card-label">Created</div>
                    <div class="info-card-value">{created_str}</div>
                </div>
                <div class="info-card">
                    <div class="info-card-label">Last Login</div>
                    <div class="info-card-value">{last_login_str}</div>
                </div>
                """, unsafe_allow_html=True)
            with col3:
                st.markdown('<div class="info-card-label" style="margin-bottom:0.3rem;">Default Role</div>', unsafe_allow_html=True)
                role_options = ["—"] + sorted(user_roles) if user_roles else ["—"]
                current_default = str(u["default_role"] or "")
                if current_default and current_default not in role_options:
                    role_options.insert(1, current_default)
                idx = role_options.index(current_default) if current_default in role_options else 0
                new_default = st.selectbox("Default Role", options=role_options, index=idx, key="manage_default_role", label_visibility="collapsed")
                if new_default != current_default and new_default != "—":
                    session.sql(f"ALTER USER {selected_user} SET DEFAULT_ROLE = {new_default}").collect()
                    st.success(f"Default role set to {new_default}")
                    st.experimental_rerun()
                status = "Disabled" if u["disabled"] == "true" else "Active"
                status_class = "disabled" if status == "Disabled" else "active"
                st.markdown(f"""
                <div class="info-card">
                    <div class="info-card-label">Status</div>
                    <div class="info-card-value {status_class}">{status}</div>
                </div>
                """, unsafe_allow_html=True)
    except Exception:
        st.caption("Could not load user info.")

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

    if active_domains and env_order:
        section("Current Roles")
        render_readonly_domains(active_domains, env_order, current_grants_by_env,
                                f"cur_{selected_user}", show_role_names=False)
    elif env_order:
        st.info(f"**{selected_user}** has no domain access in the selected environments.")

    section("Domain Access")
    st.caption("*Select domains and access levels to grant.*")
    # Domain multiselect labels show green/white dot based on existing access.
    domain_labels_with_status = ["All Domains"]
    label_to_key = {}
    for k, v in DOMAINS.items():
        has_any = False
        if env_order and current_grants_by_env:
            for env_name in env_order:
                if current_grants_by_env.get(env_name, {}).get(k, "none") != "none":
                    has_any = True
                    break
        lbl = f"🟢 {v['label']}" if has_any else f"⚪ {v['label']}"
        domain_labels_with_status.append(lbl)
        label_to_key[lbl] = k
    selected_domains = st.multiselect("Select domains", options=domain_labels_with_status, key="manage_domains")

    all_domains_selected = "All Domains" in selected_domains
    if all_domains_selected:
        modify_domains = list(DOMAINS.keys())
    else:
        modify_domains = [label_to_key[lbl] for lbl in selected_domains if lbl in label_to_key]

    new_selections = {}

    if all_domains_selected and env_order:
        bulk_level = st.radio(
            "Access for All Domains",
            options=["Reader (Select)", "Admin (Full Access)"],
            key="manage_all_domains_level",
            horizontal=True
        )
        bulk = "select" if "Reader" in bulk_level else "all"

        manage_prefix = f"manage_{selected_user}"
        render_domain_grid(modify_domains, env_order, new_selections, default_level=bulk,
                           key_prefix=manage_prefix, current_grants_by_env=current_grants_by_env, allow_none=True)
    elif modify_domains and env_order:
        manage_prefix = f"manage_{selected_user}"
        render_domain_grid(modify_domains, env_order, new_selections,
                           key_prefix=manage_prefix, current_grants_by_env=current_grants_by_env, allow_none=True)

    if new_selections and env_order:
        user_comment = st.text_input("Comment (optional)", key="manage_comment")
        if st.button("Apply Changes", type="primary"):
            role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
            any_change = False
            # Detect changes per domain, apply grants, and log each change individually.
            for env_name in env_order:
                env_suffix = ENVS[env_name]
                current_grants = current_grants_by_env.get(env_name, get_domain_grants(selected_user, env_suffix))
                changes = {}
                for domain in modify_domains:
                    old = current_grants.get(domain, "none")
                    new = new_selections.get(env_name, {}).get(domain, old)
                    if old != new:
                        apply_domain_grants(selected_user, domain, new, env_suffix, old)
                        prefix = DOMAINS[domain]["prefix"]
                        new_label = "Reader" if new == "select" else "Admin"
                        old_label = "Reader" if old == "select" else "Admin"
                        role_name = f"{prefix}_READER{env_suffix}" if new == "select" else f"{prefix}_ADMIN{env_suffix}"
                        domain_tag = f"{prefix}{env_suffix}" if env_suffix else prefix
                        if old == "none":
                            auto_comment = f"Add grants {new_label} on {domain_tag}."
                        elif new == "none":
                            auto_comment = f"Remove grants {old_label} on {domain_tag}."
                            role_name = f"{prefix}_READER{env_suffix}" if old == "select" else f"{prefix}_ADMIN{env_suffix}"
                        else:
                            auto_comment = f"Update grants to {new_label} on {domain_tag}."
                        comment_text = user_comment if user_comment.strip() else auto_comment
                        changes[domain] = {"from": old, "to": new, "role": role_name, "wh_role": f"{prefix}_WH{env_suffix}_USER", "comment": comment_text}

                if changes:
                    any_change = True
                    for domain, detail in changes.items():
                        log_action("UPDATE_GRANTS", [selected_user], role_used, env_name, [domain],
                                   {domain: detail}, detail["comment"])

            if any_change:
                st.success(f"Access updated for {selected_user}")
                st.experimental_rerun()
            else:
                st.info("No changes detected.")

    is_disabled = False
    try:
        is_disabled = user_info[0]["disabled"] == "true" if user_info else False
    except Exception:
        pass

    if is_disabled:
        section("Enable User")
        confirm_key = f"confirm_enable_{selected_user}"
        confirm = st.checkbox(f"I confirm I want to enable user {selected_user}", key=confirm_key)
        if st.button("Enable User", type="primary", disabled=not confirm):
            role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
            session.sql(f"ALTER USER {selected_user} SET DISABLED = FALSE").collect()
            log_action("ENABLE_USER", [selected_user], role_used, "ALL", ["N/A"],
                       {"N/A": {"role": "", "wh_role": ""}}, f"Enable user {selected_user}.")
            get_users.clear()
            st.success(f"{selected_user} has been re-enabled.")
            st.experimental_rerun()
    else:
        section("Disable User")
        confirm_key = f"confirm_disable_{selected_user}"
        confirm = st.checkbox(f"I confirm I want to disable user {selected_user}", key=confirm_key)
        if st.button("Disable User", type="primary", disabled=not confirm):
            role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
            session.sql(f"ALTER USER {selected_user} SET DISABLED = TRUE").collect()
            log_action("DISABLE_USER", [selected_user], role_used, "ALL", ["N/A"],
                       {"N/A": {"role": "", "wh_role": ""}}, f"Disable user {selected_user}.")
            get_users.clear()
            st.warning(f"{selected_user} has been disabled.")
            st.experimental_rerun()


def page_audit():
    all_users = get_users()

    section("Audit Search")
    selected_user = st.selectbox("Search user", options=[""] + all_users, index=0, key="audit_user")

    if not selected_user:
        return

    col_f1, col_f2, col_f3 = st.columns(3)
    with col_f1:
        action_filter = st.selectbox("Action Type", ["All", "CREATE_USER", "UPDATE_GRANTS", "DISABLE_USER"], key="audit_action")
    with col_f2:
        env_filter = st.selectbox("Environment", ["All", "DEV", "UAT", "PROD"], key="audit_env")
    with col_f3:
        object_filter = st.selectbox("Object Type", ["All", "DATABASE", "WAREHOUSE"], key="audit_object")

    where_clauses = [f"USER_NAME = '{selected_user}'"]
    if action_filter != "All":
        where_clauses.append(f"ACTION_TYPE = '{action_filter}'")
    if env_filter != "All":
        where_clauses.append(f"ENV = '{env_filter}'")
    if object_filter != "All":
        where_clauses.append(f"OBJECT_TYPE = '{object_filter}'")
    where_sql = "WHERE " + " AND ".join(where_clauses)

    try:
        df = session.sql(f"""
            SELECT 
                ACTION_TIMESTAMP,
                ACTION_TYPE,
                USER_NAME AS "USER",
                DOMAIN,
                ROLE_ASSIGNED,
                OBJECT_TYPE,
                ENV,
                COMMENT,
                PERFORMED_BY,
                ROLE_USED
            FROM MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG
            {where_sql}
            ORDER BY ACTION_TIMESTAMP DESC
        """).to_pandas()

        if df.empty:
            st.info(f"No audit entries found for **{selected_user}**.")
        else:
            page_size = 10
            total_rows = len(df)
            total_pages = max(1, (total_rows + page_size - 1) // page_size)
            page_num = st.session_state.get("audit_page", 0)
            if page_num >= total_pages:
                page_num = 0
                st.session_state["audit_page"] = 0
            start = page_num * page_size
            end = start + page_size

            st.dataframe(df.iloc[start:end], use_container_width=True)

            if total_pages > 1:
                col_prev, col_info, col_next = st.columns([1, 2, 1])
                with col_prev:
                    if st.button("Previous", key="audit_prev", disabled=(page_num == 0)):
                        st.session_state["audit_page"] = page_num - 1
                        st.experimental_rerun()
                with col_info:
                    st.caption(f"Page {page_num + 1} / {total_pages} ({total_rows} entries)")
                with col_next:
                    if st.button("Next", key="audit_next", disabled=(page_num >= total_pages - 1)):
                        st.session_state["audit_page"] = page_num + 1
                        st.experimental_rerun()
    except Exception as e:
        st.warning(f"Could not load audit log: {str(e)}")


st.set_page_config(page_title="User & Grants Manager", layout="wide")

st.markdown("""
<style>
    /* --- Hide anchor links --- */
    h1 a, h2 a, h3 a, h4 a, h5 a, h6 a { display: none !important; }

    /* --- Root font --- */
    html, body, [class*="css"] { font-family: 'Inter', -apple-system, BlinkMacSystemFont, sans-serif; }

    /* --- Section dividers --- */
    .section-divider {
        border-top: 1px solid rgba(148,163,184,0.3);
        padding-top: 0.75rem;
        margin-top: 1.25rem;
    }
    .section-divider-title {
        color: #64748B;
        font-size: 0.7rem;
        font-weight: 700;
        text-transform: uppercase;
        letter-spacing: 1.5px;
        margin: 0 0 0.5rem 0;
    }

    /* --- Header banner --- */
    .app-header {
        background: linear-gradient(135deg, #0F172A 0%, #1E293B 100%);
        padding: 1.25rem 1.75rem;
        border-radius: 10px;
        margin-bottom: 1.25rem;
        display: flex;
        align-items: center;
        justify-content: space-between;
    }
    .app-header-left { display: flex; align-items: center; gap: 0.875rem; }
    .app-header-icon {
        width: 42px; height: 42px;
        background: rgba(255,255,255,0.08);
        border: 1px solid rgba(255,255,255,0.15);
        border-radius: 10px;
        display: flex; align-items: center; justify-content: center;
        font-size: 20px;
    }
    .app-header-title { color: #F8FAFC; font-size: 1.25rem; font-weight: 700; margin: 0; line-height: 1.3; }
    .app-header-sub { color: #94A3B8; font-size: 0.75rem; margin: 0; font-weight: 400; }
    .header-badge {
        padding: 0.3rem 0.65rem;
        border-radius: 6px;
        font-size: 0.7rem;
        font-weight: 600;
        background: rgba(255,255,255,0.08);
        color: #CBD5E1;
        border: 1px solid rgba(255,255,255,0.12);
        letter-spacing: 0.3px;
    }

    /* --- Info cards --- */
    .info-card {
        background: #F8FAFC;
        border: 1px solid #E2E8F0;
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin-bottom: 0.5rem;
    }
    .info-card-label {
        font-size: 0.65rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 1px;
        color: #94A3B8;
        margin-bottom: 0.2rem;
    }
    .info-card-value {
        font-size: 0.9rem;
        font-weight: 600;
        color: #1E293B;
        font-family: 'SF Mono', 'Fira Code', monospace;
    }
    .info-card-value.active { color: #059669; }
    .info-card-value.disabled { color: #DC2626; }

    /* --- Streamlit overrides --- */
    .stTabs [data-baseweb="tab-list"] { gap: 0; border-bottom: 2px solid #E2E8F0; }
    .stTabs [data-baseweb="tab"] {
        padding: 0.6rem 1.25rem;
        font-size: 0.8rem;
        font-weight: 600;
        letter-spacing: 0.3px;
        color: #64748B;
        border-bottom: 2px solid transparent;
        margin-bottom: -2px;
    }
    .stTabs [aria-selected="true"] {
        color: #0F172A;
        border-bottom-color: #0F172A;
    }
    .stExpander {
        border: 1px solid #E2E8F0 !important;
        border-radius: 8px !important;
        margin-bottom: 0.5rem;
    }
    div[data-testid="stExpander"] details summary {
        font-weight: 600;
        font-size: 0.85rem;
    }

    /* --- Buttons --- */
    .stButton > button[kind="primary"] {
        background: #0F172A;
        border: none;
        border-radius: 8px;
        padding: 0.5rem 1.5rem;
        font-weight: 600;
        font-size: 0.8rem;
        letter-spacing: 0.3px;
        color: #FFFFFF !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: #1E293B;
        color: #FFFFFF !important;
    }
    .stButton > button[kind="primary"]:disabled {
        background: #94A3B8;
        color: #FFFFFF !important;
        opacity: 0.7;
    }

    /* --- Dataframe --- */
    .stDataFrame { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

current_role = check_access()
current_user = session.sql("SELECT CURRENT_USER()").collect()[0][0]

st.markdown(f"""
<div class="app-header">
    <div class="app-header-left">
        <div class="app-header-icon">&#x1f510;</div>
        <div>
            <div class="app-header-title">User & Grants Manager</div>
            <div class="app-header-sub">Identity & Access Governance</div>
        </div>
    </div>
    <div style="display:flex;gap:0.4rem;align-items:center;">
        <span class="header-badge">&#x1f464; {current_user}</span>
        <span class="header-badge">&#x1f511; {current_role}</span>
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