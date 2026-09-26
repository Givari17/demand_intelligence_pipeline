-- Grain: satu baris per video ulasan produk (creator vlog).
-- `product_id` dipakai untuk join ke dim_product/fact_sales kalau mau
-- analisis "apakah video review menaikkan demand" -- mart untuk itu belum
-- dibangun di sini, sengaja ditunda satu layer di atas gold ini.
select
    video_id,
    product_id,
    channel_title,
    published_at,
    view_count,
    like_count,
    comment_count,
    video_title
from {{ ref('silver_youtube_reviews') }}
