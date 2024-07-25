FROM bitnami/spark:3.5

WORKDIR /app

USER root

COPY main.py /opt/bitnami/spark/main.py

# Install necessary libraries
RUN apt-get update && apt-get install -y wget
# COPY postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

#useful for utils folder
RUN mkdir /opt/bitnami/spark/utils 
COPY utils /opt/bitnami/spark/utils/
COPY requirements.txt /opt/bitnami/spark/requirements.txt

RUN pip install -r /opt/bitnami/spark/requirements.txt

RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/mongodb/spark/mongo-spark-connector_2.12/10.3.0/mongo-spark-connector_2.12-10.3.0.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/mongodb/bson/5.1.1/bson-5.1.1.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-core/5.1.1/mongodb-driver-core-5.1.1.jar
RUN wget -P /opt/bitnami/spark/jars/ https://repo1.maven.org/maven2/org/mongodb/mongodb-driver-sync/5.1.1/mongodb-driver-sync-5.1.1.jar


# CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
# per runnare il job
ENTRYPOINT ["/opt/bitnami/spark/bin/spark-submit", "--master", "local[*]", "/opt/bitnami/spark/main.py"]