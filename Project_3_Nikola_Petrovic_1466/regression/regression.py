import sys
import numpy as np
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, isnan, isnull, count, when, min, max, avg, mean, variance, stddev, skewness, kurtosis,
    hour, trunc, round, date_format, monotonically_increasing_id, to_timestamp, desc, asc,
    year, month, dayofmonth, minute, second
)
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.regression import LinearRegression, DecisionTreeRegressor, RandomForestRegressor
from pyspark.ml import PipelineModel
from pyspark.ml import Pipeline

if __name__ == "__main__":
    # Check the number of arguments
    if len(sys.argv) < 2:
        print("Usage: <parameters> ")
        exit(-1)
    # Set a name for the application
    appName = "DataFrame Example"
    input_folder = sys.argv[1]

   
    # Create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()

    # Read in the CSV dataset as a DataFrame
    # inferSchema option forces Spark to automatically specify data column types
    # header option forces Spark to automatically fetch column names from the first line in the dataset files
    dataset = spark.read \
                .option("inferSchema", True) \
                .option("header", True) \
                .csv(input_folder)

    print("\nDataset: \n")
    dataset.show(5)

    # Select all columns except 'Duration' and move it to the end
    #dataset = dataset.select(*([col(c) for c in dataset.columns if c != 'Duration'] + [col('Duration')]))

    # Display the updated DataFrame
    #dataset.show(5)

    # Remove end date from dataset for regression model and calculating duration
    
    #dataset.show(5)

    # Check if all start stations have same start station number
    """if dataset.select('Start station').distinct().count() == dataset.select('Start station number').distinct().count():
        print('All start stations have same start station number')
    else:
        print('Start stations do not have same start station number')

    # Check if all end stations have same end station number
    if dataset.select('End station').distinct().count() == dataset.select('End station number').distinct().count():
        print('All end stations have same end station number')
    else:
        print('End stations do not have same end station number')"""

    # Drop duplicates
    #dataset = dataset.dropDuplicates()

    #dataset.describe().show()

    # Outliers tracking and deleting
    """num_rows = dataset.filter(dataset.Duration > 50000).agg(count("*")).collect()[0][0]
    print("Number of rows with Duration > 50000:", num_rows)

    total_rows = dataset.agg(count("*")).collect()[0][0]
    print("Total number of rows:", total_rows)"""


    # Calculate the mean and standard deviation of the Duration column
    """mean_val = dataset.select(mean(col('Duration'))).collect()[0][0]
    stddev_val = dataset.select(stddev(col('Duration'))).collect()[0][0]

    # Define the lower and upper bounds for outliers
    lower_bound = mean_val - 3 * stddev_val
    upper_bound = mean_val + 3 * stddev_val

    # Filter the DataFrame to find outliers
    outliers = dataset.filter((col('Duration') < lower_bound) | (col('Duration') > upper_bound))

    # Count the number of outliers
    num_outliers = outliers.count()

    print("Number of outliers in the 'Duration' column: ", num_outliers)

    non_outliers = dataset.where((col('Duration') >= 500) & (col('Duration') <= 2500))
    # Count the number of non-outliers
    num_non_outliers = non_outliers.count()

    print("Number of non-outliers in the 'Duration' column: ", num_non_outliers)

    dataset=non_outliers"""
    dataset=dataset.where((col('Duration') >= 500) & (col('Duration') <= 2500))
    dataset = dataset.drop("End date")
    dataset = dataset.drop("Start station number")
    dataset = dataset.drop("End station number")

    dataset = dataset.withColumn("Start year", year("Start date")) \
                     .withColumn("Start month", month("Start date")) \
                     .withColumn("Start day", dayofmonth("Start date")) \
                     .withColumn("Start hour", hour("Start date")) \
                     .withColumn("Start minute", minute("Start date")) \
                     .withColumn("Start second", second("Start date")) \
                     .drop("Start date")



    # Define the indexers
    start_station_indexer = StringIndexer(inputCol="Start station", outputCol="start_station_index", handleInvalid="keep")
    end_station_indexer = StringIndexer(inputCol="End station", outputCol="end_station_index", handleInvalid="keep")
    member_type_indexer = StringIndexer(inputCol="Member type", outputCol="member_type_index", handleInvalid="keep")
    bike_number_indexer = StringIndexer(inputCol="Bike number", outputCol="Bike_number_index", handleInvalid="keep")

    # Define the pipeline stages
    pipeline = Pipeline(stages=[start_station_indexer, end_station_indexer, member_type_indexer, bike_number_indexer])

    # Fit the pipeline to the dataset
    fitted_pipeline = pipeline.fit(dataset)

    # Save the fitted pipeline to HDFS
    fitted_pipeline.write().overwrite().save("hdfs://namenode:9000/dir/modelIndexer")

    indexer_model = PipelineModel.load("hdfs://namenode:9000/dir/modelIndexer")
    dataset = indexer_model.transform(dataset)

    dataset = dataset.drop("Start station")
    dataset = dataset.drop("End station")
    dataset = dataset.drop("Member type")
    dataset = dataset.drop("Bike number")


    dataset = dataset.select(*([col(c) for c in dataset.columns if c != 'Duration'] + [col('Duration')]))
    dataset.show(5)
    # Split the dataset into train, test, and validation sets

    dataset_copy = dataset.alias("dataset_copy")

    # Select all columns except the target (Duration)
    feature_cols = dataset_copy.columns[:-1]

    # Create a VectorAssembler instance to combine the feature columns
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

    # Transform the DataFrame to include the combined feature column
    dataset_copy = assembler.transform(dataset_copy)
    dataset_copy.show(5)

    # Split the dataset into train and test sets
    train_data, validation_data = dataset_copy.randomSplit([0.8, 0.2], seed=123)

    
    """from pyspark.ml.feature import StandardScaler, PCA

    # Ne daje bolje rezultate, cak stavise daje losije
    # Add PCA
    pca = PCA(k=10, inputCol="features", outputCol="pca_features")

    # Create a StandardScaler object
    scaler = StandardScaler(inputCol="pca_features", outputCol="scaled_features")
    # Define the pipeline stages
    pipeline = Pipeline(stages=[pca, scaler])

    # Fit the pipeline to the dataset
    scaler_model = pipeline.fit(train_data)

    # Save the fitted pipeline to HDFS
    scaler_model.write().overwrite().save("hdfs://namenode:9000/dir/modelScaler")

    scaler_model = PipelineModel.load("hdfs://namenode:9000/dir/modelScaler")
    # Scale the training data
    train_data = scaler_model.transform(train_data)
    # Scale the test data using the fitted scaler model
    test_data = scaler_model.transform(test_data)"""
    

    # Create a LinearRegression instance
    rf = RandomForestRegressor(featuresCol="features", labelCol="Duration", numTrees=10, maxBins=10000)
    #lr = LinearRegression(featuresCol="features", labelCol="Duration")
    #lr.setMaxIter(5)

    pipeline = Pipeline(stages=[rf])

    # Fit the model to the train data
    lr_model = pipeline.fit(train_data)

    # Evaluate the LinearRegression model

    def evaluate_model(model_name, model, validation_data):
        # Make predictions on the test data
        predictions = model.transform(validation_data)

        #print("Root Mean Squared Error (RMSE) of %s on test data = %g" % (model_name, rmse))
        eval = RegressionEvaluator(labelCol = 'Duration', predictionCol="prediction")
        rmse = eval.evaluate(predictions, {eval.metricName:'rmse'})
        r2 =eval.evaluate(predictions,{eval.metricName:'r2'})
        print(model_name, "RMSE: %.2f" %rmse)
        #print("MAE: %.2f" %mae)
        print(model_name, "R2: %.2f" %r2)
    evaluate_model("Random forest regression", lr_model, validation_data)
    lr_model.write().overwrite().save("hdfs://namenode:9000/dir/modelData")


    # Create a DecisionTreeRegressor instance with maxBins=5000
    """dt = DecisionTreeRegressor(featuresCol="features", labelCol="Duration", maxBins=5000)

    # Fit the model to the train data
    dt_model = dt.fit(train_data)

    # Evaluate the DecisionTreeRegressor model
    evaluate_model("Decision Tree Regressor", dt_model, test_data)

    # Create a RandomForestRegressor instance
    rf = RandomForestRegressor(featuresCol="features", labelCol="Duration", numTrees=10, maxBins=5000)

    # Fit the model to the train data
    rf_model = rf.fit(train_data)

    # Evaluate the RandomForestRegressor model
    evaluate_model("Random Forest Regressor", rf_model, test_data)"""
    print("END OF THE PROGRAM!!!")
    spark.stop()