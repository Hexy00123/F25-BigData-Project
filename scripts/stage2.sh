#!/bin/bash

echo "INFO: Running Stage 2 - Hive table creation + partitioning"

password=$(head -n 1 secrets/.hive.pass)

# Run the HiveQL script
echo "INFO: Executing db.hql via Beeline"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p "$password" -f sql/db.hql > output/hive_results.txt 2> /dev/null

echo "INFO: Stage 2 completed — results saved to output/hive_results.txt"
