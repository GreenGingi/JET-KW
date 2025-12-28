{{ config(materialized='table') }}

with base as (
  select *
  from {{ source('clean_xkcd', 'comics') }}
),

-- Deterministic pseudo-random numbers derived from comic_num
rng as (
  select
    comic_num,

    -- r_views in [0, 1)
    mod(abs(farm_fingerprint(cast(comic_num as string))), 1000000) / 1000000.0 as r_views,

    -- r_reviews in [0, 1) (different seed so it's not identical to views)
    mod(abs(farm_fingerprint(concat(cast(comic_num as string), '-review'))), 1000000) / 1000000.0 as r_reviews
  from base
),

final as (
  select
    b.comic_num,
    b.title,
    b.safe_title,
    b.alt,
    b.transcript,
    b.img,
    b.link,
    b.news,
    b.post_date,
    b.year,
    b.month,
    b.day,

    -- letters only (A-Z, a-z). Adjust to include accented letters if you want later.
    length(regexp_replace(coalesce(b.title, ''), r'[^A-Za-z]', '')) as title_letter_count,

    cast(5 * length(regexp_replace(coalesce(b.title, ''), r'[^A-Za-z]', '')) as numeric) as cost_eur,

    cast(round(r.r_views * 10000) as int64) as views,

    cast(1 + (r.r_reviews * 9) as numeric) as customer_review,

    current_timestamp() as model_loaded_at
  from base b
  join rng r using (comic_num)
)

select * from final;
