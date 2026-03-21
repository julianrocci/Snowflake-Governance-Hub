Best practices:

To avoid ppl doing "USE SECONDARY ROLES NONE;
" at every connexion.

You should configure it by default
USE ROLE SECURITYADMIN;

ALTER USER TOTO SET DEFAULT_SECONDARY_ROLES = ('ALL');
