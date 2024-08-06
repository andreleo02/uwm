SPARK_MASTER_URL=spark://spark-master:7077
PROGRAM_PATH=main.py

#docker compose up --build -d
docker compose exec spark-master spark-submit --master $SPARK_MASTER_URL $PROGRAM_PATH