############ Machine Learning #############
from pyspark.sql.functions import udf
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col, to_timestamp, date_format, when, split, col, concat_ws
from pyspark.sql.types import DoubleType,StringType
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
import ast
import logging, time, json

from pyspark.sql.functions import col, udf, to_timestamp, date_format
from pyspark.sql.types import StructType, StructField, DoubleType
import ast
import logging

def apply_longlag_transformations(logger, df):
    """
    
    COnvert the column lat_long containing a json to two columns
    
    """
    logger.info("Starting apply_longlag_transformations")
    
    df = df.withColumn("latitude", split(col("lat_long"), ", ")[0])
    df = df.withColumn("longitude", split(col("lat_long"), ", ")[1])

    # Cast to double if required
    df = df.withColumn("latitude", col("latitude").cast("double"))
    df = df.withColumn("longitude", col("longitude").cast("double"))

    df = df.dropna(subset=["latitude", "longitude"])
    logger.info(f"LATLONG COUNT {df.count()} entries")

    # print("apply longlag", df.count())


    return df

def extract_date(json_str):
    try:
        return json.loads(json_str).get("$date", json_str)
    except:
        return json_str

extract_date_udf = udf(extract_date, StringType())

def apply_datetime_transformations(logger, df):
    """
    PARSE TIMESTAMP
    """
    logger.info("Starting apply_datetime_transformations")
    print("TRYING")
    df = df.dropna("all", subset=["time"])
    
    if 'time' in df.columns:
        time_column_type = df.schema['time'].dataType.simpleString()
        logger.info(f"Time column type: {time_column_type}")
        print(f"Time column type: {time_column_type}")

        if time_column_type == 'struct<$date:string>':
            # Handle time as struct with $date field
            df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
        else:
            # Handle mixed formats
            df = df.withColumn("time_extracted", extract_date_udf(col("time")))
            df = df.withColumn("time", to_timestamp(col("time_extracted"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
            df = df.drop("time_extracted")

    print("DONE")

    #df.select("time").show(truncate=False)
    print("apply datetime", df.count())
    # logger.info("Starting apply_datetime_transformations")
    
    # print("TRYING")
    # df = df.dropna("all", subset=["time"])
    # if 'time' in df.columns:
    #     if df.schema['time'].dataType.simpleString() == 'struct<$date:string>':
    #         # Handle time as struct with $date field
    #         df = df.withColumn("time", to_timestamp(col("time.$date"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))
    #     else:
    #         # Handle time as string
    #         df = df.withColumn("time", to_timestamp(col("time"), 'yyyy-MM-dd\'T\'HH:mm:ss\'Z\''))

    # print("DONE")

    df = df.dropna("all", subset=["time"])

    # df.select("time").show(truncate=False)

    # print("apply datetime", df.count())

    logger.info(f"Schema after datetime transformations: {df.schema}")
    logger.info(f"Row count after transformations: {df.count()}")
    return df

def main_cleaning(logger, df_bins, df_weather):
    logger.info("Main cleaning")

    #df_bins = df_bins.dropna("any", subset=["lat_long", "time"])
    #df_weather = df_weather.dropna("any", subset=["lat_long", "time"])

    df_bins = apply_datetime_transformations(logger, df_bins)
    df_bins = apply_longlag_transformations(logger, df_bins)
    logger.info(f"df bins LATLONG COUNT {df_bins.count()} entries")

    df_weather = apply_datetime_transformations(logger, df_weather)
    df_weather = apply_longlag_transformations(logger, df_weather)
    
    # Additional cleaning specific to df_weather
    df_weather = df_weather.dropDuplicates(["time"]).drop("battery", "dev_id", "sensor_name","date", "time_only","latitude", "longitude","_id","lat_long")
    df_bins = df_bins.withColumnRenamed("temperature", "bin_temperature")

    logger.info("Completed cleaning")
    logger.info(f"Final bins schema: {df_bins.schema}")
    logger.info(f"Final weather schema: {df_weather.schema}")

    return df_bins, df_weather


def main_merging(logger, df_bins, df_weather):
    """MERGING WEATHER AND BINS DATAFRAME ON TIMESTAMP"""

    df_merged = df_bins.join(df_weather, "time", "left_outer")
    df_merged = df_merged.dropna("any")
    
    return df_merged


def main_ml(logger, spark, df_bins, df_weather):
    bins_clean, weabins_cleanther_clean = main_cleaning(logger, df_bins, df_weather)
    logger.info("Cleaning finished!!")
    df_clean = main_merging(logger, bins_clean, weabins_cleanther_clean)

    logger.info("Merging finished")

    logger.info("Number of rows after cleaning:", df_clean.count())

    df_clean = df_clean.filter((col("fill_level") >= 0) & (col("fill_level") <= 100))


    # feature_cols = ['bin_temperature', 'battery', 'latitude', 'longitude', 'solarpanel', 'solar',
    #                  'precipitation', 'strikes', 'windspeed', 'vapourpressure', 'atmosphericpressure',
    #                  'relativehumidity', 'airtemp']

    # assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    # df_features = assembler.transform(df_clean)

    # train_data, test_data = df_features.randomSplit([0.8, 0.2], seed=1234)

    # rf = RandomForestRegressor(featuresCol="features", labelCol="fill_level")
    # rf_model = rf.fit(train_data)
    # predictions = rf_model.transform(test_data)

    # evaluator = RegressionEvaluator(labelCol="fill_level", predictionCol="prediction", metricName="rmse")
    # rmse = evaluator.evaluate(predictions)
    # print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")
    # print("Predictions: ", predictions.show())

    from prophet import Prophet
    import matplotlib.pyplot as plt

    # Prepare the data for time series analysis
   # Prepare the data for time series analysis
   # Remove rows with NaN values in specified columns

    # Confirm that there are no NaN values in the specified columns
    # Remove rows with NaN values in specified columns
    # Remove rows with NaN values in specified columns
    columns_to_check = ['latitude', 'longitude', 'bin_temperature', 'battery', 'solarpanel', 'solar', 
                        'precipitation', 'strikes', 'windspeed', 'vapourpressure', 'atmosphericpressure', 
                        'relativehumidity', 'airtemp']
    df_clean = df_clean.dropna(subset=columns_to_check)

    logger.info("Number of rows after removing NaNs: %d", df_clean.count())

    # Confirm that there are no NaN values in the specified columns
    nan_counts = df_clean.select([col(c).isNull().alias(c) for c in columns_to_check]).groupby().sum().collect()[0].asDict()
    print("NaN counts after dropping NaNs:", nan_counts)

    # Prepare the data for time series analysis
    df_clean = df_clean.withColumnRenamed("time", "ds").withColumnRenamed("fill_level", "y")
    
    # Select relevant columns including ds, y, and other features
    feature_cols = ['bin_temperature', 'battery', 'latitude', 'longitude', 'solarpanel', 'solar',
                    'precipitation', 'strikes', 'windspeed', 'vapourpressure', 'atmosphericpressure',
                    'relativehumidity', 'airtemp']
    
    # Convert the DataFrame to Pandas
    df_clean_pd = df_clean.select("ds", "y", *feature_cols).toPandas()

    # Remove any remaining NaN values in the pandas DataFrame
    df_clean_pd = df_clean_pd.dropna(subset=feature_cols)
    print("NaN counts in pandas DataFrame after dropping NaNs:", df_clean_pd.isna().sum().to_dict())

    # Initialize the Prophet model
    model = Prophet()

    # Add additional regressors
    for feature in feature_cols:
        model.add_regressor(feature)

    # Fit the Prophet model
    model.fit(df_clean_pd)

    # Make future predictions for the next 12 hours
    future_12_hours = model.make_future_dataframe(periods=12, freq='H')

    # Include the future values for regressors
    # Using the last known values for simplicity
    last_values = df_clean_pd[feature_cols].tail(1)
    for feature in feature_cols:
        future_12_hours[feature] = last_values[feature].values[0]

    future_12_hours = future_12_hours.dropna(subset=feature_cols)  # Ensure no NaNs in future DataFrame
    print("NaN counts in future DataFrame after adding regressors:", future_12_hours.isna().sum().to_dict())

    forecast_12_hours = model.predict(future_12_hours)

    # Evaluate the model (using RMSE for consistency)
    df_forecast = forecast_12_hours[['ds', 'yhat']].set_index('ds')
    df_actual = df_clean_pd.set_index('ds')

    # Merge actual and forecast data
    df_merged = df_actual.join(df_forecast, how='left')

    # Calculate RMSE for the 12-hour forecast
    rmse = ((df_merged['y'] - df_merged['yhat']) ** 2).mean() ** 0.5
    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    # Plot the forecast for the next 12 hours and save the plot
    forecast_fig = model.plot(forecast_12_hours)
    plt.xlim(future_12_hours['ds'].min(), future_12_hours['ds'].max())  # Limit x-axis to the future prediction period
    plt.savefig('forecast_12_hours_plot.png')

    # Plot the forecast components and save the plot
    components_fig = model.plot_components(forecast_12_hours)
    plt.savefig('forecast_12_hours_components_plot.png')

    return model, forecast_12_hours


#main_ml(spark, df_bins, df_weather)

########### ziopera  #################

from pyspark.sql import SparkSession
spark = SparkSession.builder \
    .appName("Read JSON in Spark") \
    .getOrCreate()
df_bins = spark.read.json("spark/utils/bins.json")
#df_bins.show()
# print("df_bins schema: ", df_bins.printSchema())
df_weather = spark.read.json("spark/utils/weather.json")
#df_weather.show()
# print("df_weather schema: ", df_weather.printSchema())

###################################################################################àà

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# bins_clean, weabins_cleanther_clean = main_cleaning(logger, df_bins, df_weather)
# logger.info("Cleaning finished!!")
#bins_clean.show()
#weabins_cleanther_clean.show()
# df_clean = main_merging(logger, bins_clean, weabins_cleanther_clean)
# df_clean.show()
#main_ml(logger,spark, df_bins, df_weather)
main_ml(logger,spark, df_bins, df_weather)