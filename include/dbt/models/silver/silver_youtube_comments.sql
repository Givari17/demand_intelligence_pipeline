-- Silver: dedup per comment_id, buang komentar kosong.
with deduped as (
    select *, row_number() over (
        partition by comment_id order by ingestion_timestamp desc
    ) as rn
    from {{ ref('stg_youtube_comments') }}
    where comment_id is not null
)
select comment_id, video_id, topic_id, comment_text, like_count, published_at
from deduped
where rn = 1
  and comment_text is not null
  and trim(comment_text) <> ''
