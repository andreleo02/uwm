FROM bitnami/spark:3.5

USER root

COPY main.py /opt/bitnami/spark/main.py

COPY postgresql-42.7.3.jar /opt/bitnami/spark/jars/postgresql-42.7.3.jar

COPY mongo-spark-connector_2.13-10.1.1.jar /opt/bitnami/spark/jars/mongo-spark-connector_2.13-10.1.1.jar

#useful for utils folder
RUN mkdir  /opt/bitnami/spark/utils 
COPY utils /opt/bitnami/spark/utils/
COPY requirements.txt /opt/bitnami/spark/requirements.txt

RUN pip install -r requirements.txt

CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
# per runnare il job
#CMD ["spark-submit", "/opt/bitnami/spark/main.py"]