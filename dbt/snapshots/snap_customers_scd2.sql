{% snapshot snap_customers_scd2 %}

{{
    config(
      target_schema='analytics',
      unique_key='customer_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    city,
    state,
    country,
    postal_code,
    customer_segment,
    created_at,
    updated_at
FROM {{ ref('stg_retail__customers') }}

{% endsnapshot %}

