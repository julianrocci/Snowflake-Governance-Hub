with users as (
    select * from {{ ref('stg_app_data__users') }}
),

transactions as (
    select * from {{ ref('stg_app_data__transactions') }}
),

joined as (
    select
        t.transaction_id,
        t.user_id,
        u.country_code,
        u.signup_date,
        t.purchase_date,
        -- Truncate dates to the first day of the month to group by month
        date_trunc('month', u.signup_date) as cohort_month,
        date_trunc('month', t.purchase_date) as purchase_month,
        t.transaction_amount_chf
    from transactions t
    left join users u 
        on t.user_id = u.user_id
),

calculated_intervals as (
    select
        *,
        -- Calculate the number of months between signup and purchase
        datediff('month', cohort_month, purchase_month) as month_number
    from joined
)

select * from calculated_intervals