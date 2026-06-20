with source as (
    select * from {{ source('app_data_source', 'raw_users') }}
),

renamed as (
    select
        user_id as user_id,
        signup_date::date as signup_date,
        country as country_code
    from source
)

select * from renamed