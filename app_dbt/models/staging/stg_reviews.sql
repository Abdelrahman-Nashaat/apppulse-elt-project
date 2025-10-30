{{ config(
    materialized='table'
) }}

-- Reads data from the reviews_from_mongo.csv seed file
WITH source AS (

    SELECT *
    FROM {{ ref('reviews_from_mongo') }} -- <<< Reads from the reviews seed file

),

cleaned AS (

    SELECT
        -- Standardize column names to match Star Schema
        "App" AS app_name,
        "Translated_Review" AS review_text,
        "Sentiment" AS review_sentiment,
         -- Cast to appropriate types, handling potential errors/NULLs represented as strings
        TRY_CAST(NULLIF(CAST("Sentiment_Subjectivity" AS VARCHAR), 'NULL') AS DOUBLE) AS subjectivity,
        TRY_CAST(NULLIF(CAST("Sentiment_Polarity" AS VARCHAR), 'NULL') AS DOUBLE) AS polarity,
        -- Generate a unique key for each review
        md5(cast(coalesce(cast("App" as TEXT), '_') || coalesce(cast("Translated_Review" as TEXT), '_') as TEXT)) as source_unique_key


    FROM
        source
    WHERE
        "Translated_Review" IS NOT NULL -- Filter out empty reviews
)

SELECT * FROM cleaned