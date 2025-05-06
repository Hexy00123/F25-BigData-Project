USE team11_projectdb;

DROP TABLE IF EXISTS mq_model_2;

CREATE EXTERNAL TABLE mq_model_2 (
    label DOUBLE,
    prediction DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 'hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/output/model2_predictions/'
TBLPROPERTIES ("skip.header.line.count"="1");
