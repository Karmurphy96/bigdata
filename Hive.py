CREATE EXTERNAL TABLE IF NOT EXISTS flattened_reviews (
    rating FLOAT,
    title STRING,
    text STRING,
    asin STRING,
    parent_asin STRING,
    user_id STRING,
    `timestamp` BIGINT,
    helpful_vote INT,
    verified_purchase BOOLEAN,
    image_count INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION 'gs://bigdataecommerce/reviews_data/'
TBLPROPERTIES ("skip.header.line.count"="1");