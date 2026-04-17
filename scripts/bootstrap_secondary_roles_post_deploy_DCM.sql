-- ============================================================
-- ONE-SHOT SECONDARY ROLES SETUP
-- Run once. Enables all secondary roles for
-- every user so database roles + account roles work together
-- in the same session without manual USE SECONDARY ROLES ALL.
-- ============================================================

-- Apply to all existing users via a scripted loop
DECLARE
    user_name VARCHAR;
    c CURSOR FOR SELECT "name" FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE "deleted_on" IS NULL;
BEGIN
    FOR record IN c DO
        user_name := record."name";
        EXECUTE IMMEDIATE 'ALTER USER ' || :user_name || ' SET DEFAULT_SECONDARY_ROLES = (''ALL'')';
    END FOR;
END;