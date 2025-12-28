-- create datasets
CREATE SCHEMA IF NOT EXISTS `raw_xkcd`;
CREATE SCHEMA IF NOT EXISTS `clean_xkcd`;

-- store JSON payload + minimal metadata
CREATE TABLE IF NOT EXISTS `raw_xkcd.comics_json` (
  ingested_at TIMESTAMP NOT NULL,
  source STRING NOT NULL,              
  comic_num INT64,                     -- extracted in Python for convenience
  payload JSON NOT NULL                -- full JSON response, to be unraveled in the `processed_xkcd` table
);
-- note the 'source' column doesn't have much use for this exercise, but in a real world scenario it would provide information on how a row was added
-- e.g. 'current' if the data was fetched by this script, 'backfill' if we had to backfill data with a new script, ect...
-- this is for the purpose of troubleshooting in case of a bad update

-- create clean table
CREATE TABLE IF NOT EXISTS `clean_xkcd.comics` (
  comic_num INT64 NOT NULL,
  title STRING,
  safe_title STRING,
  alt STRING,
  transcript STRING,
  img STRING,
  link STRING,
  news STRING,
  post_date DATE,                      -- derived from year/month/day
  year INT64,
  month INT64,
  day INT64,
  row_update TIMESTAMP NOT NULL
)
PARTITION BY post_date
CLUSTER BY comic_num;
