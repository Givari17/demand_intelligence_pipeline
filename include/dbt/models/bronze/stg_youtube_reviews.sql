-- Bronze: parsing tipe data dari payload JSON video ulasan produk (creator
-- vlog) via YouTube API, tanpa dedup/business rule.
-- CATATAN: skema payload di bawah ini masih ASUMSI -- _ingest_youtube_reviews
-- di dags/demand_intelligence_dag.py masih placeholder (belum ada pemanggilan
-- API asli), jadi sesuaikan field ini begitu payload sebenarnya diketahui,
-- terutama `product_id` (hasil mapping manual video -> produk yang direview,
-- bukan field bawaan dari YouTube API).
select
    source_record_id as video_id,
    batch_id,
    ingestion_timestamp,
    record_timestamp,
    payload ->> '$.product_id'    as product_id,
    payload ->> '$.channel_title' as channel_title,
    (payload ->> '$.published_at')::timestamp as published_at,
    (payload ->> '$.view_count')::integer    as view_count,
    (payload ->> '$.like_count')::integer    as like_count,
    (payload ->> '$.comment_count')::integer as comment_count,
    payload ->> '$.title'         as video_title
from {{ source('raw', 'bronze_youtube_reviews') }}
