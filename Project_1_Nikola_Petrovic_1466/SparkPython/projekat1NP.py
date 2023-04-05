import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, hour, trunc, round, stddev, min, max, mean, avg, col, isnan, when, count, isnull, variance, skewness, kurtosis, date_format, monotonically_increasing_id, to_timestamp, desc, asc

if __name__ == "__main__":
    #check the number of arguments
    if len(sys.argv) < 2:
        print("Usage: projekat1NP.py <parameters> ")
        exit(-1)
    #Set a name for the application
    appName = "DataFrame Example"
    input_folder = sys.argv[1]
    input_station = sys.argv[2]
    input_date1 = sys.argv[3]
    input_date2 = sys.argv[4]
    duration=sys.argv[5]
    print("appName:", appName)
    print("input_folder:", input_folder)
    print("input_station:", input_station)
    print("input_date1:", input_date1)
    print("input_date2:", input_date2)
    print("duration:", duration)
    #create a new Spark application and get the Spark session object
    spark = SparkSession.builder.appName(appName).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    #read in the CSV dataset as a DataFrame
    #inferSchema option forces Spark to automatically specify data column types
    #header option forces Spark to automatically fetch column names from the first line in the dataset files
    dataset = spark.read \
                .option("inferSchema", True) \
                .option("header", True) \
                .csv(input_folder)

    #dataset.write.csv("C:/Users/Nikola Petrovic/Desktop/bigdata/kafka/kafka-spark-flink-container-main/data/bikes.csv", header=True, mode="overwrite")
    

    print("Count of dataset:"+str(dataset.count()))
    print("\nNumber of columns:"+str(len(dataset.columns)))

    print("\nDataset: \n")
    dataset.show(5)


    dataset.select(min("duration"), max("duration"), mean("duration"), stddev("duration")).show(10)

    # Use input timestamp in filter conditions
    print("\n Station details:\n")
    result = dataset.filter((col("Start station") == input_station))
    # Show only top 5 rows
    result.show(5)

    print("\n Station details from-to date:\n")
    result = result.filter((col("Start date") >= lit(input_date1)) & (col("Start date") <= lit(input_date2)))
    # Show only top 5 rows

    result.show(5)

    rows = result.count()
    print(f"\nNumber of rides from{input_date1} to {input_date2} : {rows}\n")


    result = dataset.filter((col("Start station") != input_station) & (col("Duration") > duration))
    result.show(5)

    rows = result.count()
    print(f"\nNumber of rides with duration greater than {duration} : {rows}\n")

    dataset = dataset.withColumn("startDay", date_format(col("Start date"), "EEEE"))
    dataset = dataset.withColumn("endDay", date_format(col("End date"), "EEEE"))
    print("\nAdding columns start day and end day to dataframe\n")
    dataset.show(5, False)
    # Most common days for ride in two ways
    # I
    most_popular_day = dataset.groupBy("startDay").count().orderBy(desc("count")).first()["startDay"]

    # Most common days for ride
    # II
    days = dataset.groupBy("startDay").agg(count("*").alias("count"))
    print("\nPopularity of a day for biking\n")
    days = days.sort(desc("count"))
    days.show()
    print("People in Washington likes to drive bike in the middle of the week")
    print("\n Most popular day:\n" + most_popular_day)

    # ### where usually people taking bikes?

    # Group the data by the Start station attribute
    start_station_grouped = dataset.groupBy("Start station")

    # Count the number of occurrences of each Start station
    start_station_counts = start_station_grouped.agg(count("*").alias("count"))

    # Sort the Start station by the number of occurrences in descending order
    sorted_start_station = start_station_counts.sort(col("count").desc())

    # The most popular Start station will be the first row of the sorted counts
    most_popular_start_station = sorted_start_station.first()["Start station"]
    print("Popularity of start stations:\n")
    sorted_start_station.show(10, False)
    print("Most popular start station:\n")
    print(most_popular_start_station)

    # Extract the hour of the day from the "Start date" column
    dataset = dataset.withColumn("hour", hour(col("Start date")))
    # Group the data by the hour of the day
    grouped_data = dataset.groupBy("hour")
    # Count the number of bike rides for each hour
    ride_counts = grouped_data.agg(count("*"))
    # Divide the counts by the number of hours in a day
    avg_rides = ride_counts.withColumn("avg_rides", col("count(1)") / 24)
    # Round the avg_rides column to 2 decimal places
    avg_rides = avg_rides.withColumn("avg_rides", round(col("avg_rides"), 2))
    # Sort the hours by the average number of bike rides in descending order
    sorted_avg_rides = avg_rides.sort(col("avg_rides").desc())
    # The most popular hours will be the first rows of the sorted averages
    most_popular_hours = sorted_avg_rides.select("hour", "avg_rides")
    # Display the data in tabular format with hour and average number of bike rides for each hour
    print("\nDisplay the sorted data in tabular format with hour and average number of bike rides for each hour\n")
    most_popular_hours.show()

"""
    print("\n Showing number of null values for each column")
    dataset.select([count(when(col(c).isNull(), c)).alias(c) for c in dataset.columns]).show()



    df2 = dataset.select([count(when(col(c).contains('None') | \
                                     col(c).contains('NULL') | \
                                     (col(c) == '') | \
                                     col(c).isNull(), c)).alias(c)
                          for c in dataset.columns])
    df2.show()

    # %%
    rows = dataset.count()
    print(f"DataFrame Rows count : {rows}")

    # Get columns count
    cols = len(dataset.columns)
    print(f"DataFrame Columns count : {cols}")

    # dataset.describe()

    # remove rows with null
    print("\nremove rows with null\n")
    dataframe = dataset.dropna()
    dataframe.show(3)

    # Duplicates
    import pyspark.sql.functions as funcs

    print("\nCount duplicates\n")
    dataset.groupBy(dataset.columns).count().where(funcs.col('count') > 1).select(funcs.sum('count')).show()

    # Find numeric and categorical column
    numeric_columns = list()
    categorical_column = list()
    for col_ in dataset.columns:
        if dataset.select(col_).dtypes[0][1] != "string":
            numeric_columns.append(col_)
        else:
            categorical_column.append(col_)

    print("Numeric columns", numeric_columns)
    print("Categorical columns", categorical_column)

    # ### Advanced pyspark for Exploratory Data Analysis

    #dataset.show(10, False)

    print("\nAdding id column\n")
    dataset = dataset.withColumn("id", monotonically_increasing_id())
    dataset.show(4)

    # Moving column id to the start
    print("\nMoving id column to start\n")
    dataset = dataset.select("id", *[col for col in dataset.columns if col != "id"])
    dataset.show(3)

    # Showing some columns
    print("\nShowing some columns\n")
    result = dataset.select("Start station", "End station")
    result.show(5, False)

    #import pandas as pd
    print("\nShow columns where start date is after some value, and duration is bigger than some value\n")
    result = dataset.filter((col("Duration") > 2700) & (col("Start date") > "2017-07-01 00:01:00"))
    result.show(5, False)

    print("\nShowing number of rides that are in borders of requirements\n")
    print(result.count())

    # Sort data from first start ascending
    print("\nSort data in ascending from first ride \n")
    dataset.sort(col("Start date").asc()).show(5, truncate=False)

    #dataset.show(5, False)

    import pyspark.sql.functions as sparkFun
    print("\nShow stations and average durations of rides started from that station\n")
    result = dataset.groupBy("Start station").agg(sparkFun.avg("Duration"))
    result.show(5, False)


    # Show number of bikers with membership
    print("\nShow number of bikers with membership\n")
    member_type_counts = dataset.groupBy("Member type").agg(count("*").alias("count"))
    member_type_counts.show()

    # Here is the complete code to find the bike numbers that hold the record between every two stations:

    # grouping the data by Bike number, Start station, and End station
    record_holder_bikes = dataset.groupBy("Bike number", "Start station", "End station").agg(
        max("Duration").alias("max_duration"))

    # sorting the result by max_duration
    record_holder_bikes = record_holder_bikes.sort(desc("max_duration"))

    # displaying the result
    print("\nDisplay top 10 record holder bikes for max duration\n")
    record_holder_bikes.show(10, False)

    # grouping the data by Bike number, Start station, and End station
    record_holder_bikes = dataset.groupBy("Bike number", "Start station", "End station").agg(
        min("Duration").alias("min_duration"))

    # sorting the result by min_duration
    record_holder_bikes = record_holder_bikes.sort(asc("min_duration"))

    # displaying the result
    print("\nDisplay top 10 record holder bikes for min duration\n")
    record_holder_bikes.show(10, False)


    most_used_bike = dataset.groupBy("Bike number").agg(count("*").alias("count"))
    most_used_bike = most_used_bike.sort(desc("count"))
    print("\nMost used bikes\n")
    most_used_bike.show(5, False)

    # Drop columns
    # dataset = dataset.drop("startDay", "day")


    dataset = dataset.withColumn("startDay", date_format(col("Start date"), "EEEE"))
    dataset = dataset.withColumn("endDay", date_format(col("End date"), "EEEE"))
    print("\nAdding columns start day and end day to dataframe\n")
    dataset.show(5, False)

    # Most common days for ride in two ways
    # I

    most_popular_day = dataset.groupBy("startDay").count().orderBy(desc("count")).first()["startDay"]

    # Most common days for ride
    # II


    days = dataset.groupBy("startDay").agg(count("*").alias("count"))

    print("\nPopularity of a day for biking\n")
    days = days.sort(desc("count"))

    days.show()

    print("People in Washington likes to drive bike in the middle of the week")
    print("\n Most popular day:\n"+most_popular_day)

    # ### where usually people taking bikes?

    # Group the data by the Start station attribute
    start_station_grouped = dataset.groupBy("Start station")

    # Count the number of occurrences of each Start station
    start_station_counts = start_station_grouped.agg(count("*").alias("count"))

    # Sort the Start station by the number of occurrences in descending order
    sorted_start_station = start_station_counts.sort(col("count").desc())

    # The most popular Start station will be the first row of the sorted counts
    most_popular_start_station = sorted_start_station.first()["Start station"]
    print("Popularity of start stations:\n")
    sorted_start_station.show(10, False)
    print("Most popular start station:\n")
    print(most_popular_start_station)



    # Now for leaving bikes
    # Group the data by the End station attribute
    end_station_grouped = dataset.groupBy("End station")

    # Count the number of occurrences of each End station
    end_station_counts = end_station_grouped.agg(count("*").alias("count"))

    # Sort the End station by the number of occurrences in descending order
    sorted_end_station = end_station_counts.sort(col("count").desc())

    # The most popular End station will be the first row of the sorted counts
    most_popular_end_station = sorted_end_station.first()["End station"]

    print("Popularity of end stations:\n")
    sorted_end_station.show(10, False)
    print("Most popular end station:\n")
    print(most_popular_end_station)

    # Extract the hour of the day from the "Start date" column
    dataset = dataset.withColumn("hour", hour(col("Start date")))

    # Group the data by the hour of the day
    grouped_data = dataset.groupBy("hour")

    # Count the number of bike rides for each hour
    ride_counts = grouped_data.agg(count("*"))

    # Divide the counts by the number of hours in a day
    avg_rides = ride_counts.withColumn("avg_rides", col("count(1)") / 24)

    # Round the avg_rides column to 2 decimal places
    avg_rides = avg_rides.withColumn("avg_rides", round(col("avg_rides"), 2))

    # Sort the hours by the average number of bike rides in descending order
    sorted_avg_rides = avg_rides.sort(col("avg_rides").desc())

    # The most popular hours will be the first rows of the sorted averages
    most_popular_hours = sorted_avg_rides.select("hour", "avg_rides")

    # Display the data in tabular format with hour and average number of bike rides for each hour
    print("\nDisplay the sorted data in tabular format with hour and average number of bike rides for each hour\n")
    most_popular_hours.show()

    #1. Odrediti broj i karakteristike odgovarajućih vrednosti atributa/događaja na određenoj
    #lokaciji (oblasti) u određenom vremenskom priodu (koji se zadaju kao parametri
    #aplikacije), koji zadovoljavaju zadati uslov



    #grouped_data = dataset.groupBy("hour")
    #ride_counts = grouped_data.agg("Minimum", )



"""