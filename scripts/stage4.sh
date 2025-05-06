#!/bin/bash

echo "INFO: Running stage 4"

password=$(head -n 1 secrets/.hive.pass)

echo "INFO: Creating mq1 table"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p "$password" -f sql/mq1.hql > /dev/null

echo "INFO: Creating mq_model_1 table"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p "$password" -f sql/mq_model_1.hql > /dev/null

echo "INFO: Creating mq_model_2 table"
beeline -u jdbc:hive2://hadoop-03.uni.innopolis.ru:10001 -n team11 -p "$password" -f sql/mq_model_2.hql > /dev/null