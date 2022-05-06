
/*
This file contains all the queries which are utilized to prepare the data for Quicksight dashboard
These queries were run in Athena and the data was sourced from S3 buckets
*/

-----------------------------------------------------------------------------------------------------
-- 1. Query to create tweets table from using S3 bucket where the scraped data is saved
-----------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `wcd-database-wk`.`tweets` (
  `id` string,
  `username` string,
  `screen_name` string,
  `tweet` string,
  `followers_count` int,
  `location` string,
  `geo` string,
  `created_at` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '	',
  'field.delim' = '	',
  'collection.delim' = '.',
  'mapkey.delim' = '.'
) LOCATION 's3://wcd-bucket-wk/PHEV/2022/04/22/'
TBLPROPERTIES ('has_encrypted_data'='false');


-----------------------------------------------------------------------------------------------------
-- 2a. Query to retrieve predictions table from predictions parquet file as generated from Databrick 
--    machine learning model output and stored in a S3 bucket
-----------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `wcd-database-wk`.`predictions` (
  `id` bigint,
  `tweet` string,
  `sentiment` string,
  `tokens` array<string>,
  `filtered` array<string>,
  `cv` map<string, string>,
  `features` map<string, string>,
  `label` double,
  `rawprediction` map<string, string>,
  `probability` map<string, string>,
  `prediction` double
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://wcd-bucket-wk/PHEV/predictions.parquet/'
TBLPROPERTIES ('has_encrypted_data'='false');

-----------------------------------------------------------------------------------------------------
-- 2b. There was an issue joining tables inside quickinsight on id columns because. The schema in 
--     databricks for 'id' was set as bigint where as in quickinsight it had to be string based on
--     our schema in query 1 above. So as a quick fix we created another view where we case id as 
--     sting and then load inside quicksight for further analysis. This was just a quick fix, ideally
--     we would refine our schema in query 1 above and in databricks consitently so that there is no
--     need for this step
-----------------------------------------------------------------------------------------------------
CREATE or REPLACE VIEW "final_predictions" AS
SELECT *,
CAST(id AS varchar) AS id_as_string
FROM predictions;



-----------------------------------------------------------------------------------------------------
-- 3. Query to retrieve a list of commonly used words for creating a wordcloud. This list is created 
--    inside Databricks and is saved as a separated parquet file in S3. In Athena we create a separate
--    database wordcloud for this table
-----------------------------------------------------------------------------------------------------
CREATE EXTERNAL TABLE IF NOT EXISTS `wordcloud`.`wordlist` (
  `WordsColumn` string
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe' 
WITH SERDEPROPERTIES (
  'serialization.format' = '1'
) LOCATION 's3://wcd-bucket-wk/PHEV/words.parquet/'
TBLPROPERTIES ('has_encrypted_data'='false');


-----------------------------------------------------------------------------------------------------
-- 4. Query to create a country column using the location column in the tweets table. We extract
--    country name strings from location string. We do it for top 20 countries by Twitter usage
-----------------------------------------------------------------------------------------------------
CREATE or REPLACE VIEW "tweetloc" AS
SELECT id, location, created_at,

CASE 
WHEN LOWER(location) LIKE '%united states%' OR 
     LOWER(location) LIKE '%us%' THEN 'United States'
WHEN LOWER(location) LIKE '%japan%' THEN 'Japan'
WHEN LOWER(location) LIKE '%India%' THEN 'India'
WHEN LOWER(location) LIKE '%brazil%' OR
     LOWER(location) LIKE '%brasil%' THEN 'Brazil'
WHEN LOWER(location) LIKE '%indonesia%' THEN 'Indonesia'
WHEN LOWER(location) LIKE '%united kingdom%' THEN 'United Kingdom'
WHEN LOWER(location) LIKE '%turkey%' THEN 'Turkey'
WHEN LOWER(location) LIKE '%saudi%' THEN 'Saudi Arabia'
WHEN LOWER(location) LIKE '%mexico%' THEN 'Mexico'
WHEN LOWER(location) LIKE '%thai%' THEN 'Thailand'
WHEN LOWER(location) LIKE '%philip%' THEN 'Philippines'
WHEN LOWER(location) LIKE '%france%' THEN 'France'
WHEN LOWER(location) LIKE '%spain%' THEN 'Spain'
WHEN LOWER(location) LIKE '%canada%' THEN 'Canada'
WHEN LOWER(location) LIKE '%germany%' THEN 'Germany'
WHEN LOWER(location) LIKE '%south korea%' THEN 'South Korea'
WHEN LOWER(location) LIKE 'argentina%' THEN 'Argentina'
WHEN LOWER(location) LIKE '%egypt%' THEN 'Egypt'
WHEN LOWER(location) LIKE '%malyasia%' THEN 'Malaysia'
WHEN LOWER(location) LIKE '%colombia%' THEN 'Colombia'
END as Country

FROM tweets;

