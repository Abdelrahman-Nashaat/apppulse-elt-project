{{ config(
    materialized='table'
) }}

-- هذا النموذج ينشئ جدول أبعاد للتصنيفات والأنواع (Categories and Genres)
-- يتم استخدام دالة التجميع (GROUP BY) للحصول على جميع القيم الفريدة.

select
    -- توليد مفتاح رئيسي (category_id) باستخدام دالة MD5 Hash لتصنيف فريد
    -- يضمن أن كل مجموعة فريدة من (Category, Genres) تحصل على مفتاحها الخاص.
    md5(cast(coalesce(cast(Category as string), '_') || '_' || coalesce(cast(Genres as string), '_') as string)) as category_id,
    
    Category as app_category,
    Genres as app_genres

from
    {{ ref('stg_apps') }}

where
    Category is not null

group by
    1, 2, 3
