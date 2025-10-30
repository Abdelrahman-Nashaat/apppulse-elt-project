{{ config(
    materialized='table'
) }}

-- Creates the Category Dimension table

SELECT
    -- Generate a surrogate key using the unique combination of category and genres
    md5(cast(coalesce(cast(app_category as TEXT), '_') || coalesce(cast(app_genres as TEXT), '_') as TEXT)) as category_id,
    app_category,
    app_genres

FROM {{ ref('stg_apps') }}
WHERE app_category IS NOT NULL
GROUP BY -- Group by category and genres to get unique combinations
    app_category,
    app_genres