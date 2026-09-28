-- Grain: satu baris per video. Konteks pasar standalone (tidak di-join ke
-- product_id). Bisa dibandingkan per topic_id / periode dengan fact_sales
-- hanya sebagai pembanding tren, bukan analisis kausal.
select video_id, topic_id, search_query, channel_title, video_title,
       published_at, view_count, like_count, comment_count
from {{ ref('silver_youtube_videos') }}
