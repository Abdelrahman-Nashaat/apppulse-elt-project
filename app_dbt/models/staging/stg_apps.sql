{{ config(
    materialized='table'
) }}

-- Reads data from the apps_from_mysql.csv seed file
WITH source AS (

    SELECT *
    FROM {{ ref('apps_from_mysql') }} -- <<< Reads from the app seed file

),

cleaned AS (
    SELECT
        -- Standardize column names (Ensure these match the columns in apps_from_mysql.csv)
        "App" AS app_name,
        "Category" AS app_category,
        -- Rating is already FLOAT NULL from Python script
        "Rating" AS app_rating,
        "Reviews" AS reviews_text,
        "Size" AS size_text,
        -- Installs is already BIGINT from Python script
        "Installs" AS installs_int,
        "Type" AS app_type,
        "Price" AS price_text,
        "Content_Rating" AS content_rating,
        "Genres" AS app_genres,
        -- Attempt to cast Last_Updated to DATE, handle potential 'Unknown' or other non-date strings
        TRY_CAST("Last_Updated" AS DATE) AS last_updated_date,
        "Current_Ver" AS current_version,
        "Android_Ver" AS android_version,
        -- Generate a unique key for the source row
        md5(cast(coalesce(cast("App" as TEXT), '_') || coalesce(cast("Last_Updated" as TEXT), '_') || coalesce(cast("Current_Ver" as TEXT), '_') as TEXT)) as source_unique_key

    FROM
        source
    WHERE
        "App" IS NOT NULL -- Basic filter
)
-- Add final type conversions and cleaning here
SELECT
    app_name,
    app_category,
    app_rating,
    reviews_text,
     -- Convert Size to bytes (handle M and k, and 'Varies with device')
    CASE
        WHEN size_text = 'Varies with device' THEN NULL
        WHEN size_text LIKE '%M' THEN TRY_CAST(REPLACE(size_text, 'M', '') AS DOUBLE) * 1024 * 1024
        WHEN size_text LIKE '%k' THEN TRY_CAST(REPLACE(size_text, 'k', '') AS DOUBLE) * 1024
        ELSE NULL -- Handle cases that don't match M or k
    END AS app_size_bytes,
    installs_int,
    app_type,
    -- Convert Price to numeric (handle '$' and '0')
    CASE
        WHEN price_text = '0' THEN 0.0
        ELSE price_text
    END AS app_price,
    content_rating,
    app_genres,
    last_updated_date,
    current_version,
    android_version,
    source_unique_key
FROM cleaned
WHERE installs_int IS NOT NULL -- Ensure installs are valid numbers