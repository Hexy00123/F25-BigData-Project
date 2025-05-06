#!/bin/bash

echo "INFO: Running Stage 2 - Hive table creation + partitioning"

password=$(head -n 1 secrets/.hive.pass)

# Run the HiveQL script
echo "INFO: Executing db.hql via Beeline"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p "$password" -f sql/db.hql > output/hive_results.txt 2> /dev/null

echo "INFO: Executing qx.hql via Beeline"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q1.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q1.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q2.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q2.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q3.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q3.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q4.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q4.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q5.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q5.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q6.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q6.csv
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p lNNSIZJcy4QwC9XL -f sql/q7.hql --hiveconf hive.resultset.use.unique.column.names=false > output/q7.csv


echo "INFO: Stage 2 completed — results saved to output/hive_results.txt"
