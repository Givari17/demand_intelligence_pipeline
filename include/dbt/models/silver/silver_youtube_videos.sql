-- Silver: dedup per video_id (ambil ingestion terbaru, statistik view/like
-- paling mutakhir).
with deduped as (
    select *, row_number() over (
        partition by video_id order by ingestion_timestamp desc
    ) as rn
    from {{ ref('stg_youtube_videos') }}
    where video_id is not null
)
select video_id, topic_id, search_query, channel_title, video_title,
       published_at, view_count, like_count, comment_count
from deduped
where rn = 1
