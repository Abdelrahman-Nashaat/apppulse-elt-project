{{ config(
    materialized='table'
) }}

-- Creates the Application Dimension table

SELECT
    -- Generate a surrogate key for the app dimension
    md5(cast(coalesce(cast(app_name as TEXT), '_') || coalesce(cast(current_version as TEXT), '_') as TEXT)) as app_id, 
    app_name,
    -- developer_name is not available from stg_apps based on current structure, remove or add to stg_apps
    app_size_bytes,
    app_price,
    content_rating,
    last_updated_date,
    current_version,
    android_version,  -- ✅ fixed: replaced required_android_version with android_version
    source_unique_key -- To link back to staging if needed
    -- Add other descriptive fields from stg_apps if available

FROM {{ ref('stg_apps') }}
GROUP BY 
    app_name,
    app_size_bytes,
    app_price,
    content_rating,
    last_updated_date,
    current_version,
    android_version,   -- ✅ same fix in GROUP BY
    source_unique_key
