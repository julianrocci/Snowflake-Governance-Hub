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

ALLOWED_ROLES = ("ACCOUNTADMIN", "SECURITYADMIN", "SYSADMIN")


def check_access():
    role = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
    if role not in ALLOWED_ROLES:
        st.error(f"Accès refusé. Rôle actuel : **{role}**. Seuls {', '.join(ALLOWED_ROLES)} peuvent accéder à cette application.")
        st.stop()
    return role


def get_env_suffix():
    db = session.sql("SELECT CURRENT_DATABASE()").collect()[0][0]
    if db.endswith("_DEV"):
        return "_DEV"
    elif db.endswith("_UAT"):
        return "_UAT"
    else:
        return ""


def log_action(action_type, target_users, role_used, domains_affected, grants_detail, comment=""):
    users_arr = ", ".join([f"'{u}'" for u in target_users])
    domains_arr = ", ".join([f"'{d}'" for d in domains_affected]) if domains_affected else ""
    grants_json = json.dumps(grants_detail) if grants_detail else "{}"
    session.sql(f"""
        INSERT INTO MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG 
        (ACTION_TYPE, TARGET_USERS, PERFORMED_BY, ROLE_USED, DOMAINS_AFFECTED, GRANTS_DETAIL, COMMENT)
        SELECT 
            '{action_type}',
            ARRAY_CONSTRUCT({users_arr}),
            CURRENT_USER(),
            '{role_used}',
            ARRAY_CONSTRUCT({domains_arr}),
            PARSE_JSON('{grants_json}'),
            '{comment}'
    """).collect()


def get_users():
    rows = session.sql("SHOW USERS IN ACCOUNT").collect()
    return [r["name"] for r in rows]


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
            select_val = current in ("select", "all")
            all_val = current == "all"
            sel = st.checkbox("Select", value=select_val, key=f"{key_prefix}_{domain}_select")
            adm = st.checkbox("All", value=all_val, key=f"{key_prefix}_{domain}_all")
            if adm:
                selections[domain] = "all"
            elif sel:
                selections[domain] = "select"
            else:
                selections[domain] = "none"
    return selections


def page_create():
    st.header("Créer des utilisateurs")
    st.markdown("Saisissez un ou plusieurs noms d'utilisateur séparés par des virgules.")

    usernames_input = st.text_input("Noms d'utilisateur", placeholder="USER1, USER2, USER3")
    default_role = st.selectbox("Rôle par défaut", ["PUBLIC", "FIN_READER", "MKT_READER"], index=0)

    st.subheader("Droits par domaine")
    selections = render_domain_checkboxes("create")

    if st.button("Créer les utilisateurs", type="primary"):
        if not usernames_input.strip():
            st.error("Veuillez saisir au moins un nom d'utilisateur.")
            return

        usernames = [u.strip().upper() for u in usernames_input.split(",") if u.strip()]
        env_suffix = get_env_suffix()
        role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]

        progress = st.progress(0)
        created = []
        errors = []

        for idx, username in enumerate(usernames):
            try:
                session.sql(f"CREATE USER IF NOT EXISTS {username} DEFAULT_ROLE = '{default_role}' MUST_CHANGE_PASSWORD = TRUE PASSWORD = 'TempPass123!'").collect()

                for domain, level in selections.items():
                    if level != "none":
                        apply_domain_grants(username, domain, level, env_suffix, "none")

                created.append(username)
            except Exception as e:
                errors.append(f"{username}: {str(e)}")

            progress.progress((idx + 1) / len(usernames))

        domains_affected = [d for d, l in selections.items() if l != "none"]
        grants_detail = {d: l for d, l in selections.items() if l != "none"}

        if created:
            log_action("CREATE_USER", created, role_used, domains_affected, grants_detail,
                       f"Created {len(created)} user(s) with domain grants")
            st.success(f"✅ {len(created)} utilisateur(s) créé(s) : {', '.join(created)}")

        if errors:
            for err in errors:
                st.error(err)


def page_manage():
    st.header("Gérer les utilisateurs")

    all_users = get_users()
    search = st.text_input("Rechercher un utilisateur", placeholder="Commencez à taper...")

    if search:
        filtered = [u for u in all_users if search.upper() in u.upper()]
    else:
        filtered = all_users

    if not filtered:
        st.info("Aucun utilisateur trouvé.")
        return

    selected_user = st.selectbox("Sélectionner un utilisateur", filtered)

    if selected_user:
        env_suffix = get_env_suffix()
        current_grants = get_domain_grants(selected_user, env_suffix)
        all_roles = get_user_roles(selected_user)

        with st.expander("Rôles actuels", expanded=False):
            if all_roles:
                for r in sorted(all_roles):
                    st.code(r)
            else:
                st.info("Aucun rôle assigné.")

        st.subheader("Modifier les droits")
        new_selections = render_domain_checkboxes(f"manage_{selected_user}", current_grants)

        col1, col2 = st.columns(2)
        with col1:
            if st.button("Appliquer les modifications", type="primary"):
                role_used = session.sql("SELECT CURRENT_ROLE()").collect()[0][0]
                changes = {}
                for domain in DOMAINS:
                    old = current_grants.get(domain, "none")
                    new = new_selections[domain]
                    if old != new:
                        apply_domain_grants(selected_user, domain, new, env_suffix, old)
                        changes[domain] = {"from": old, "to": new}

                if changes:
                    has_any_grant = any(v != "none" for v in new_selections.values())
                    action_type = "DISABLE_USER" if not has_any_grant else "UPDATE_GRANTS"
                    domains_affected = list(changes.keys())

                    log_action(action_type, [selected_user], role_used, domains_affected, changes,
                               f"{'Disabled' if not has_any_grant else 'Updated grants for'} {selected_user}")

                    if not has_any_grant:
                        st.warning(f"⚠️ {selected_user} n'a plus aucun droit — marqué comme désactivé.")
                    else:
                        st.success(f"✅ Droits mis à jour pour {selected_user}")

                    st.rerun()
                else:
                    st.info("Aucune modification détectée.")

        with col2:
            if st.button("Réinitialiser"):
                st.rerun()


def page_logs():
    st.header("Journal d'activité")
    rows = session.sql("""
        SELECT ACTION_ID, ACTION_TYPE, TARGET_USERS, PERFORMED_BY, ROLE_USED, 
               DOMAINS_AFFECTED, GRANTS_DETAIL, COMMENT, ACTION_TIMESTAMP
        FROM MGMT_DB.USER_MANAGEMENT.USER_ACTIVITY_LOG
        ORDER BY ACTION_TIMESTAMP DESC
        LIMIT 100
    """).to_pandas()

    if rows.empty:
        st.info("Aucune activité enregistrée.")
    else:
        st.dataframe(rows, use_container_width=True)


st.set_page_config(page_title="User & Grants Manager", layout="wide")
st.title("User & Grants Manager")

current_role = check_access()
st.caption(f"Connecté en tant que **{session.sql('SELECT CURRENT_USER()').collect()[0][0]}** | Rôle : **{current_role}**")

tab1, tab2, tab3 = st.tabs(["➕ Créer", "✏️ Gérer", "📋 Journal"])

with tab1:
    page_create()
with tab2:
    page_manage()
with tab3:
    page_logs()