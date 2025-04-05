#!/bin/bash

bash scripts/data_collection.sh
bash scripts/data_storage.sh
password=$(head -n 1 secrets/.psql.pass)

echo "INFO: Clear hdfs warehouse folder"
hdfs dfs -rm -r project/warehouse/taxi_trips

echo "INFO: Importing data via sqoop"
sqoop import-all-tables --connect jdbc:postgresql://hadoop-04.uni.innopolis.ru/team11_projectdb --username team11 --password $password --compression-codec=snappy --compress --as-avrodatafile --warehouse-dir=project/warehouse --m 1

echo "INFO: Move *.avsc & *.java to output"
mv *.avsc output/
mv *.java output/