{% snapshot snap_products_scd2 %}

{{
    config(
      target_schema='analytics',
      unique_key='product_id',
      strategy='timestamp',
      updated_at='updated_at',
      invalidate_hard_deletes=True
    )
}}

SELECT
    product_id,
    product_name,
    category,
    subcategory,
    unit_price,
    cost_price,
    gross_margin_usd,
    sku,
    is_active,
    created_at,
    updated_at
FROM {{ ref('stg_retail__products') }}

{% endsnapshot %}

