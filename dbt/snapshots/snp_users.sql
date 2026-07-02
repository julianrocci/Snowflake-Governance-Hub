-- Snapshot: Tracks SCD Type 2 changes on user data (country changes)

{% snapshot snp_users %}

{{
    config(
        target_schema='snapshots',
        unique_key='user_id',
        strategy='check',
        check_cols=['country', 'plan', 'status']
    )
}}

select * from {{ source('app_data', 'raw_users') }}

{% endsnapshot %}