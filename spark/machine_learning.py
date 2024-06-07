"""


"""

## DA AGGIUSTARE CON LA ROBA NOSTRAAAAAAAAAAAAA

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors
from pyspark.ml.regression import LinearRegression
from pyspark.sql import Row

# Create a SparkSession
spark = SparkSession.builder \
    .appName("My App") \
    .getOrCreate()

print("SPARK SESSION CREATED SUCCESSFULLY")

log_file = "/opt/bitnami/spark/logs/app.log"

with open(log_file, "w") as file:
    file.write("MARDEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEEN")
    file.write("SPARK SESSION CREATED SUCCESSFULLY\n")
    
    # Create some example data for linear regression
    data = [
        Row(label=1.0, features=Vectors.dense(0.0)),
        Row(label=2.0, features=Vectors.dense(1.0)),
        Row(label=3.0, features=Vectors.dense(2.0)),
        Row(label=4.0, features=Vectors.dense(3.0))
    ]
    
    # Create a DataFrame from the example data
    df = spark.createDataFrame(data)
    print("DATAFRAME CREATED SUCCESSFULLY")
    # Create a Linear Regression model
    lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)
    
    # Fit the model
    lr_model = lr.fit(df)
    
    # Print the coefficients and intercept for linear regression
    file.write(f"Coefficients: {lr_model.coefficients}\n")
    file.write(f"Intercept: {lr_model.intercept}\n")
    
    # Summarize the model over the training set and print out some metrics
    training_summary = lr_model.summary
    file.write(f"NumIterations: {training_summary.totalIterations}\n")
    file.write(f"ObjectiveHistory: {training_summary.objectiveHistory}\n")
    file.write(f"RMSE: {training_summary.rootMeanSquaredError}\n")
    file.write(f"r2: {training_summary.r2}\n")
    print("LINEAR REGRESSION MODEL TRAINED SUCCESSFULLY")
# Stop the SparkSession
spark.stop()
