-- Bronze: parsing tipe data payload video YouTube (konteks pasar), tanpa dedup.
select
    source_record_id as video_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.topic_id'      as topic_id,
    payload ->> '$.query'         as search_query,
    payload ->> '$.channel_title' as channel_title,
    payload ->> '$.title'         as video_title,
    (payload ->> '$.published_at')::timestamp as published_at,
    try_cast(payload ->> '$.view_count' as bigint)    as view_count,
    try_cast(payload ->> '$.like_count' as bigint)    as like_count,
    try_cast(payload ->> '$.comment_count' as bigint) as comment_count
from {{ source('raw', 'bronze_youtube_videos') }}
