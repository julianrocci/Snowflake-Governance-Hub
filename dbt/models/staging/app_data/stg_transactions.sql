with source as (
    select * from {{ source('app_data_source', 'raw_transactions') }}
),

renamed as (
    select
        transaction_id as transaction_id,
        user_id as user_id,
        purchase_date::date as purchase_date,
        amount as transaction_amount_chf
    from source
)

select * from renamed