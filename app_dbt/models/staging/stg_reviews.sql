{{ config(
    materialized='table'
) }}

-- قراءة البيانات تتم الآن من ملف Seed reviews_from_mongo.csv الذي تم إنشاؤه بواسطة سكريبت Python
with source as (

    -- 'reviews_from_mongo' هو اسم ملف الـ CSV بدون اللاحقة (.csv) الموجود في مجلد seeds
    select *
    from {{ ref('reviews_from_mongo') }}

),

cleaned as (

    select 
        -- توحيد أسماء الأعمدة لتتناسب مع الـ Star Schema
        "App" as app_name, 
        "Translated_Review" as review_text,
        "Sentiment" as review_sentiment,
        "Sentiment_Subjectivity" as subjectivity,
        "Sentiment_Polarity" as polarity
    from 
        source
    where
        "Translated_Review" is not null -- تصفية المراجعات الفارغة
)

select * from cleaned
