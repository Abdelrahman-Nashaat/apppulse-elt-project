{{ config(
    materialized='table'
) }}

-- يدمج هذا النموذج البيانات من stg_apps و stg_reviews لإنشاء جدول الحقائق.
-- يتم حساب متوسط التقييمات والمشاعر (Sentiment) على مستوى التطبيق.

with 

-- 1. دمج بيانات التطبيقات مع مفاتيح الأبعاد (تم دمجها الآن في subquery 3)

-- 2. حساب متوسط المشاعر (Sentiment) من جدول المراجعات
review_metrics as (
    select
        -- تم التعديل للإشارة إلى اسم العمود app_name في stg_reviews
        t.app_name, 
        avg(t.polarity) as avg_sentiment_polarity 
    from
        {{ ref('stg_reviews') }} as t
    where
        t.polarity is not null 
    group by
        1
)

-- 3. بناء جدول الحقائق النهائي
select
    -- المفاتيح الخارجية
    da.app_id, -- المفتاح الخارجي لـ dim_apps
    dc.category_id, -- المفتاح الخارجي لـ dim_categories
    
    -- القياسات
    ad.average_user_rating,
    ad.total_reviews,
    ad.total_installs,
    ad.app_price,
    rm.avg_sentiment_polarity, -- متوسط المشاعر

    -- بيانات إضافية (مفيدة لتحديد السياق)
    ad.source_unique_key

from
    (
        select
            -- يتم توليد نفس المفتاح الذي تم استخدامه في dim_apps و dim_categories لربطهما
            md5(cast(coalesce(cast(App as string), '_') || '_' || coalesce(cast(Category as string), '_') as string)) as source_unique_key,
            
            -- القياسات (Measures)
            t.Rating as average_user_rating,
            t.Reviews as total_reviews,
            t.Installs as total_installs,
            t.Price as app_price,
            t.App as app_name -- إضافة app_name من stg_apps لمقارنته لاحقًا
        from 
            {{ ref('stg_apps') }} as t
    ) as ad -- تم دمج app_data CTE في Subquery لتبسيط الكود

left join
    review_metrics as rm on ad.app_name = rm.app_name -- دمج متوسط المشاعر
left join
    {{ ref('dim_apps') }} as da on ad.source_unique_key = da.app_id -- دمج للحصول على app_id
left join
    {{ ref('dim_categories') }} as dc on ad.source_unique_key = dc.category_id -- دمج للحصول على category_id
where
    da.app_id is not null
