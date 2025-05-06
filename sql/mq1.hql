USE team11_projectdb;

DROP TABLE IF EXISTS mq1_results;

CREATE EXTERNAL TABLE mq1_results (
    model STRING,
    params STRING,
    rmse DOUBLE,
    r2 DOUBLE,
    train_time_sec DOUBLE,
    eval_time_sec DOUBLE
)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde'
WITH SERDEPROPERTIES (
   "separatorChar" = ",",
   "quoteChar"     = "\"",
   "escapeChar"    = "\\"
)
STORED AS TEXTFILE
LOCATION 'hdfs://hadoop-02.uni.innopolis.ru:8020/user/team11/project/output/evaluation/'
TBLPROPERTIES ("skip.header.line.count"="1");
