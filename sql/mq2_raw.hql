USE team11_projectdb;

DROP TABLE IF EXISTS mq_features_raw;

SET hive.execution.engine=mr;

CREATE EXTERNAL TABLE mq_features_raw (
    raw_data STRING
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe'
STORED AS TEXTFILE
LOCATION 'hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/data/train/';


DROP TABLE IF EXISTS mq_features;

CREATE TABLE mq_features (
    features STRING,
    target DOUBLE
);

INSERT INTO TABLE mq_features
SELECT
    get_json_object(raw_data, '$.features.values') AS features,
    cast(get_json_object(raw_data, '$.label') AS DOUBLE) AS target
FROM mq_features_raw limit 100;

SELECT * FROM mq_features limit 3;