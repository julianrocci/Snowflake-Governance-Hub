DEFINE TAG {{mgmt_db}}.CONFIG.OWNER
    ALLOWED_VALUES 'data_team', 'analytics_team', 'finance_team', 'marketing_team', 'ecommerce_team', 'retail_team', 'loyalty_team', 'platform_team'
    COMMENT = 'Team responsible for the object';

DEFINE TAG {{mgmt_db}}.CONFIG.DOMAIN
    ALLOWED_VALUES 'finance', 'marketing', 'ecommerce', 'retail', 'analytics', 'loyalty', 'management'
    COMMENT = 'Business domain the object belongs to';

DEFINE TAG {{mgmt_db}}.CONFIG.COST_CENTER
    COMMENT = 'Cost center code for chargeback — free-form to support per-team granularity';

DEFINE TAG {{mgmt_db}}.CONFIG.DATA_SENSITIVITY
    ALLOWED_VALUES 'PII', 'PCI', 'PHI', 'NONE'
    COMMENT = 'Type of sensitive data — drives column-level masking policies';

DEFINE TAG {{mgmt_db}}.CONFIG.DATA_CLASSIFICATION
    ALLOWED_VALUES 'CONFIDENTIAL', 'INTERNAL', 'PUBLIC'
    COMMENT = 'Organizational access level — drives table/schema access policies';