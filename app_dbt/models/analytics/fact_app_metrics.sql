{{ config(
    materialized='table'
) }}

-- Fact table combining app, category, and review metrics

WITH apps AS (
    SELECT
        app_id,
        app_name,
        app_price,
        content_rating,
        android_version,
        current_version,
        source_unique_key
    FROM {{ ref('dim_apps') }}
),

categories AS (
    SELECT
        app_genres,
        app_category
    FROM {{ ref('dim_categories') }}
),

reviews AS (
    SELECT
        app_name,
        -- ✅ تحويل النص إلى قيمة رقمية قبل الحساب
        AVG(
            CASE
                WHEN LOWER(review_sentiment) = 'positive' THEN 1.0
                WHEN LOWER(review_sentiment) = 'neutral' THEN 0.5
                WHEN LOWER(review_sentiment) = 'negative' THEN 0.0
                ELSE NULL
            END
        ) AS avg_sentiment,
        COUNT(*) AS total_reviews
    FROM {{ ref('stg_reviews') }}
    GROUP BY app_name
)

SELECT
    a.app_id,
    a.app_name,
    c.app_category,
    c.app_genres,
    a.app_price,
    a.content_rating,
    a.android_version,
    a.current_version,
    r.avg_sentiment,
    r.total_reviews
FROM apps a
LEFT JOIN categories c 
    ON a.app_name = c.app_genres
LEFT JOIN reviews r 
    ON a.app_name = r.app_name
