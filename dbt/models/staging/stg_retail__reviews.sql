WITH source AS (
    SELECT * FROM {{ source('retail_source', 'stg_reviews_raw') }}
),

renamed AS (
    SELECT
        TRIM(review_id) AS review_id,
        TRIM(product_id) AS product_id,
        TRIM(customer_id) AS customer_id,
        CAST(rating AS INT) AS rating,
        TRIM(review_text) AS review_text,
        CASE
            WHEN rating >= 4 THEN 'POSITIVE'
            WHEN rating = 3 THEN 'NEUTRAL'
            ELSE 'NEGATIVE'
        END AS sentiment_category,
        CAST(review_date AS TIMESTAMP) AS review_date,
        {{ audit_columns() }}
    FROM source
    WHERE review_id IS NOT NULL AND rating BETWEEN 1 AND 5
)

SELECT * FROM renamed

