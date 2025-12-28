-- insert & update the 'clean' data via MERGE
MERGE `clean_xkcd.comics` t

-- create source dataset s to merge from
USING (
  -- CTE to pre-select most recent rows
  WITH latest_raw AS (
    SELECT
      ingested_at,
      -- coalesce in case python script is unreliable
      COALESCE(comic_num, CAST(JSON_VALUE(payload, '$.num') AS INT64)) AS comic_num,
      payload
    FROM `raw_xkcd.comics_json`
    QUALIFY -- deduplication step
      ROW_NUMBER() OVER (
        PARTITION BY COALESCE(comic_num, CAST(JSON_VALUE(payload, '$.num') AS INT64))
        ORDER BY ingested_at DESC
      ) = 1
  )
  SELECT
    comic_num,
    JSON_VALUE(payload, '$.title') AS title,
    JSON_VALUE(payload, '$.safe_title') AS safe_title,
    JSON_VALUE(payload, '$.alt') AS alt,
    JSON_VALUE(payload, '$.transcript') AS transcript,
    JSON_VALUE(payload, '$.img') AS img,
    JSON_VALUE(payload, '$.link') AS link,
    JSON_VALUE(payload, '$.news') AS news,
    CAST(JSON_VALUE(payload, '$.year') AS INT64) AS year,
    CAST(JSON_VALUE(payload, '$.month') AS INT64) AS month,
    CAST(JSON_VALUE(payload, '$.day') AS INT64) AS day,
    DATE(
      CAST(JSON_VALUE(payload, '$.year') AS INT64),
      CAST(JSON_VALUE(payload, '$.month') AS INT64),
      CAST(JSON_VALUE(payload, '$.day') AS INT64)
    ) AS post_date,
    ingested_at AS loaded_at
  FROM latest_raw
) s

ON t.comic_num = s.comic_num

WHEN MATCHED THEN -- handles cases where the row already exists
  UPDATE SET
    title = s.title,
    safe_title = s.safe_title,
    alt = s.alt,
    transcript = s.transcript,
    img = s.img,
    link = s.link,
    news = s.news,
    post_date = s.post_date,
    year = s.year,
    month = s.month,
    day = s.day,
    row_update = s.loaded_at
WHEN NOT MATCHED THEN -- handles cases where the row does not exist yet
  INSERT (
    comic_num, title, safe_title, alt, transcript, img, link, news,
    post_date, year, month, day, row_update
  )
  VALUES (
    s.comic_num, s.title, s.safe_title, s.alt, s.transcript, s.img, s.link, s.news,
    s.post_date, s.year, s.month, s.day, s.loaded_at
  );
