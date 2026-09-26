-- Silver: dedup per video_id. `product_id` dibiarkan boleh null di sini
-- (belum tentu semua video sudah di-mapping ke produk saat ingestion) --
-- fact_youtube_reviews di gold tetap menyertakannya sebagai kolom nullable,
-- join ke dim_product baru valid untuk baris yang sudah ter-mapping.
with deduped as (
    select
        *,
        row_number() over (
            partition by video_id
            order by ingestion_timestamp desc
        ) as rn
    from {{ ref('stg_youtube_reviews') }}
)

select
    video_id,
    product_id,
    channel_title,
    published_at,
    view_count,
    like_count,
    comment_count,
    video_title
from deduped
where rn = 1
