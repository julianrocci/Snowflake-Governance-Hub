with user_transactions as (
    select * from {{ ref('int_user_transactions_joined') }}
),

-- Calculate the total unique users who signed up in each cohort month
cohort_sizes as (
    select
        date_trunc('month', signup_date) as cohort_month,
        count(distinct user_id) as total_cohort_users
    from {{ ref('stg_app_data__users') }}
    group by 1
),

-- Count how many unique users from each cohort returned in subsequent months
returning_users as (
    select
        cohort_month,
        month_number,
        count(distinct user_id) as active_users,
        sum(transaction_amount_chf) as total_revenue_chf
    from user_transactions
    group by 1, 2
)

-- Calculate the final retention percentage
select
    r.cohort_month,
    s.total_cohort_users,
    r.month_number,
    r.active_users,
    -- Calculate retention rate
    round((r.active_users::float / s.total_cohort_users) * 100, 2) as retention_rate_percentage,
    r.total_revenue_chf
from returning_users r
left join cohort_sizes s 
    on r.cohort_month = s.cohort_month
order by r.cohort_month, r.month_number