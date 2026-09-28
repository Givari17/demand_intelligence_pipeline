-- Grain: satu baris per komentar. Bahan analisis sentimen/willingness to pay
-- (NLP dikerjakan di luar dbt, mis. Project 2.0).
select comment_id, video_id, topic_id, comment_text, like_count, published_at
from {{ ref('silver_youtube_comments') }}
