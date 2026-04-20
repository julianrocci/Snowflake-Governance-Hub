-- ============================================================
-- ONE-SHOT SECONDARY ROLES SETUP
-- Run once. Enables all secondary roles for every user 
-- (current and future) so database roles + account roles 
-- work together in the same session.
-- ============================================================

ALTER ACCOUNT SET DEFAULT_SECONDARY_ROLES = ('ALL');