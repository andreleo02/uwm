#!/bin/bash

#chmod +x spark.sh   !!!!!!!!!!! if you dont have permission
#./spark.sh

SPARK_MASTER_URL=spark://192.168.240.2:7077
PROGRAM_PATH=machine_learning.py

docker compose exec spark-master spark-submit --master $SPARK_MASTER_URL $PROGRAM_PATH
