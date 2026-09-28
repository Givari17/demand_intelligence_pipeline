-- Bronze: parsing tipe data payload komentar YouTube, tanpa dedup.
select
    source_record_id as comment_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.video_id' as video_id,
    payload ->> '$.topic_id' as topic_id,
    payload ->> '$.text'     as comment_text,
    try_cast(payload ->> '$.like_count' as bigint) as like_count,
    (payload ->> '$.published_at')::timestamp as published_at
from {{ source('raw', 'bronze_youtube_comments') }}
