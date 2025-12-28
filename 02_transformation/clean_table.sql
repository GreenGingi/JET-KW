{{ config(materialized='view') }}

with deduped as (
  select
    ingested_at,
    source,
    cast(comic_num as int64) as comic_num,
    payload
  from {{ source('raw_xkcd', 'comics_json') }}
  qualify row_number() over (partition by comic_num order by ingested_at desc) = 1
),

parsed as (
  select
    cast(json_value(payload, '$.num') as int64) as comic_num,
    json_value(payload, '$.title') as title,
    json_value(payload, '$.safe_title') as safe_title,
    json_value(payload, '$.alt') as alt,
    json_value(payload, '$.transcript') as transcript,
    json_value(payload, '$.img') as img,
    json_value(payload, '$.link') as link,
    json_value(payload, '$.news') as news,
    cast(json_value(payload, '$.year') as int64) as year,
    cast(json_value(payload, '$.month') as int64) as month,
    cast(json_value(payload, '$.day') as int64) as day,
    date(
      cast(json_value(payload, '$.year') as int64),
      cast(json_value(payload, '$.month') as int64),
      cast(json_value(payload, '$.day') as int64)
    ) as post_date,
    current_timestamp() as loaded_at
  from deduped
)

select * from parsed;
