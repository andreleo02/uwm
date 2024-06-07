FROM bitnami/spark:latest

# Switch to root user to install dependencies
USER root

# Ensure the list directory exists and is writable, then install numpy
RUN mkdir -p /var/lib/apt/lists/partial && \
    apt-get update && \
    apt-get install -y python3-pip && \
    pip3 install numpy

# Switch back to the non-root user
USER 1001

# Copy the Python file into the container
COPY machine_learning.py /opt/bitnami/spark/machine_learning.py

# Set the working directory
WORKDIR /opt/bitnami/spark

# Specify the command to run the Spark master
CMD ["bin/spark-class", "org.apache.spark.deploy.master.Master"]
