# **New York City Taxi Data Analysis**
<br>

## **Introduction**
The project focuses on data processing and analysis using Databricks Spark, with the primary goal of leveraging Apache Spark to conduct a comprehensive analysis of a high-volume dataset. It aims to analyse a large dataset from New York City taxi trips by loading, transforming, and performing detailed analysis to derive valuable insights and predictions. Throughout the project, Databricks is utilised as a core platform to process and manipulate data, with the dataset stored in Microsoft Azure Blob Storage. Additionally, various tools and technologies are employed to process and transform data, including Python, PySpark, and Spark SQL. After being ingested from Azure, the dataset undergoes extensive cleaning to remove unrealistic records, then be explored using Spark SQL to extract insights into taxi operations, trip patterns, and passenger behaviour. By the end of the project, Spark ML pipelines will be used to build and train predictive models, with performance evaluated against a baseline to ensure accuracy in predicting trip totals.<br>
<br>

## **Key objectives**
The primary goal of the project is to conduct a comprehensive analysis of a large dataset using Apache Spark, with a focus on data ingestion, transformation, machine learning model development for predicting profound findings.<br>
<br>

## **Project workflow**
Dataset acquisition → Set up Azure Blob storage (create storage account and container) → Upload dataset to Azure → Ingest data to Databricks → Explore and manipulate data → Develop Machine Learning models <br>
<br>


## **Dataset**
The dataset for this project is provided by the New York City Taxi and Limousine Commission (TLC), a company which has been responsible for managing license and regulating New York City’s taxis since 1971. The dataset comprises
16 parquet files,representing taxi data from 2015 to 2022 of two types of taxis cabs: yellow and green taxis, and a [location data](taxi_zone_lookup.csv). The dataset for taxi cabs can be downloaded from the following links:

#### **Yellow taxi**
- [yellow_taxi_2015.parquet](https://drive.google.com/file/d/1owWyJDNTWyLT0ln2iK5ulkmSYvXZ7qkf/view)
- [yellow_taxi_2016.parquet](https://drive.google.com/file/d/1OdIcvpyFH1YXn9SNHc8YEuVFAYQpxCUw/view)
- [yellow_taxi_2017.parquet](https://drive.google.com/file/d/1rtEhtit_2rKvWgutXNIpSWPk3vuE6q8r/view)
- [yellow_taxi_2018.parquet](https://drive.google.com/file/d/1073SHSIkWcSESNZoU0JHXudRlSzJXPM9/view)
- [yellow_taxi_2019.parquet](https://drive.google.com/file/d/144mphzh2a6qerjLvCwwO_QHDsXTdNAJ3/view)
- [yellow_taxi_2020.parquet](https://drive.google.com/file/d/1kB5Bnx1TAXMq_revh1fyUU5RYOMdmIh4/view)
- [yellow_taxi_2021.parquet](https://drive.google.com/file/d/1eTs-ID9A3ZgYy0BotrEKwh9ThRAt8dfu/view)
- [yellow_taxi_2022.parquet](https://drive.google.com/file/d/1QdBDxHQzffBZ26T3j6Uhk1eJ8EmF0bCN/view)

#### **Green taxi**
- [green_taxi_2015.parquet](https://drive.google.com/file/d/137oXWkqBOQcxmgHynPv6Wh_fHUqN40n3/view)
- [green_taxi_2016.parquet](https://drive.google.com/file/d/1s0drAVqulJ_hE4RRqMSNJWyQGF6RKAtA/view)
- [green_taxi_2017.parquet](https://drive.google.com/file/d/1-VpjWArKPEdjzlTZxI7aPwd8UsVfc2bL/view)
- [green_taxi_2018.parquet](https://drive.google.com/file/d/1jDn7qjFZ3-nrn4iOdFMh_p0W21esiIHn/view)
- [green_taxi_2019.parquet](https://drive.google.com/file/d/1BpjTq89EAhb6m-ICcZMEpTISw6jTHmio/view)
- [green_taxi_2020.parquet](https://drive.google.com/file/d/1umIMHrqaqagZYqvLLf-OzidnDPwxIY5j/view)
- [green_taxi_2021.parquet](https://drive.google.com/file/d/1ISKrR97II-zWR7f2_boFcyfsNgsj8K1Y/view)
- [green_taxi_2022.parquet](https://drive.google.com/file/d/1ysXV_4hB3Ex43k1HOvCi8RCT1k7GZANj/view)
<br>


## **Tools used**
- Microsoft Azure
- Databricks
- Apache Spark
- Python
- Pyspark
- SparkSQL
<br>

## **Features**
- **Large-scale data handling**: Efficient processing of a massive dataset with approximately 800 million records, leveraging the distributed computing power of Apache Spark on Databricks
- **Integration with Azure Blob Storage**: Seamless integration with Azure Blob Storage as the primary storage solution for managing.
- **End-to-End workflow in Databricks**
- **Data cleaning and transformation**: Comprehensive data cleaning and transformation pipelines implemented using PySpark, ensuring high-quality data for analysis and modeling
- **Predictive Modeling**: Develop Linear Regression and Random Forest with the goal of helping stakeholders in understanding fare dynamics and optimizing pricing strategies
<br>

## **Data Cleaning**

#### Remove trips finishing before starting time
One necessary step in the data cleaning process was ensuring that all recorded taxi trips had valid time sequences, specifically that the drop-off time occurred after the pickup time. Any trips having the drop-off timestamp was recorded earlier than the pickup timestamp are logically inconsistent. To handle this issue, the following steps are applied:
- Convert datetime to timestamp: The pickup and drop-off datetime were initially in string format in both the yellow and green taxi datasets. These features were converted to timestamp format using `to_timepstamp()` function.
```python
# Convert pickup and dropoff datetime columns from string to timestamp
df_green = df_green.withColumn('lpep_pickup_timestamp', to_timestamp(col('lpep_pickup_datetime'), 'yyyy-MM-dd HH:mm:ss'))
df_green = df_green.withColumn('lpep_dropoff_timestamp', to_timestamp(col('lpep_dropoff_datetime'), 'yyyy-MM-dd HH:mm:ss'))

df_yellow = df_yellow.withColumn('tpep_pickup_timestamp', to_timestamp(col('tpep_pickup_datetime'), 'yyyy-MM-dd HH:mm:ss'))
df_yellow = df_yellow.withColumn('tpep_dropoff_timestamp', to_timestamp(col('tpep_dropoff_datetime'), 'yyyy-MM-dd HH:mm:ss'))
```

- A filter was applied to remove any trips where drop-off time occurred before pickup time
```python
df_green = df_green.filter(df_green['lpep_dropoff_datetime'] >= df_green['lpep_pickup_datetime'])
df_yellow = df_yellow.filter(df_yellow['tpep_dropoff_datetime'] >= df_yellow['tpep_pickup_datetime'])
```
<br>

#### Remove trips where the pickup/dropoff datetime is outside of the range

```python
# Define the valid datetime range
valid_start_date = '2015-01-01T00:00:00.000+00:00'
valid_end_date = '2022-12-31T23:59:59.999+00:00'

# Remove trips where the time is outside of the range:
# green taxi
df_green = df_green.filter((col('lpep_pickup_timestamp') >= valid_start_date) & (col('lpep_pickup_timestamp') <= valid_end_date) &
                           (col('lpep_dropoff_timestamp') >= valid_start_date) & (col('lpep_dropoff_timestamp') <= valid_end_date))

# yellow taxi
df_yellow = df_yellow.filter((col('tpep_pickup_timestamp') >= valid_start_date) & (col('tpep_pickup_timestamp') <= valid_end_date) &
                             (col('tpep_dropoff_timestamp') >= valid_start_date) & (col('tpep_dropoff_timestamp') <= valid_end_date))
```
<br>

#### Remove trips with negative speed
To handle this cleaning step, I first converted pickup and drop-off datetime to UNIX timestamp format. The reason for this transformation would be that UNIX timestamp represent time as the number of seconds, which make it easier for computing. This package was imported using the code `pyspark.sql.functions unix_timestamp`

```python
# New columns called `trip_duration` were generated in both datasets by calculating the difference between `pickup_timestamp` and `dropoff_timestamp`

## green taxi
df_green = df_green.withColumn('trip_duration', (unix_timestamp(col('lpep_dropoff_timestamp')) - unix_timestamp(col('lpep_pickup_timestamp'))))

## yellow taxi
df_yellow = df_yellow.withColumn('trip_duration', (unix_timestamp(col('tpep_dropoff_timestamp')) - unix_timestamp(col('tpep_pickup_timestamp'))))

# Remove trips having trip duration <= 0
df_green = df_green.filter(col('trip_duration') > 0)
df_yellow = df_yellow.filter(col('trip_duration') > 0)

# Calculate speed of trips
df_green = df_green.withColumn('speed', col('trip_distance')/(col('trip_duration')/3600))
df_yellow = df_yellow.withColumn('speed', col('trip_distance')/(col('trip_duration')/3600))

# Remove trips with negative speed
df_green = df_green.filter(col('speed') >= 0)
df_yellow = df_yellow.filter(col('speed') >= 0)
```
<br>

#### Remove trips with excessively high speed
Within New York City, the limit speed limit is 55 mph. I first defined a variable called speed_limit and assign it a value of 55, then remove trips having speed higher than the speed limit.


```python
speed_limit = 55
df_green = df_green.filter(col('speed') <= speed_limit)
df_yellow = df_yellow.filter(col('speed') <= speed_limit)
```
<br>

#### Remove trips with a *duration* which is either too short or too long 
Both extremely short and extremely long trips may lead to data errors since it can influence average trip duration, trip distance predictions, etc. Therefore, removing trips which have
too long or too short duration is an important cleaning step. To handle this issue, the following steps are applied:
- I first checked the minimum and maximum duration of two datasets, then I counted the number of trips which were longer than 7200-second drive to ensure not to
remove a huge amount of dataset. The result showed that only 370 thousand trips were recorded to last longer than 7200 seconds (2 hours) out of a total of over 66
million trips, hence, setting the duration limits of 2 hours may not have a significant impact on the quality of dataset.
- Define the minimum trip duration as 180 seconds (3 minutes) and define the maximum trip duration as 7200 seconds (2 hours). Any trip outside this range will be removed.

```python
from pyspark.sql.functions import min, max

# Define min and max duration for a trip
min_duration = 180 # 3 minutes
max_duration = 7200 # 2 hours

# Filter too long or too short trips
df_green = df_green.filter((col('trip_duration') >= min_duration) & (col('trip_duration') <= max_duration))
df_yellow = df_yellow.filter((col('trip_duration') >= min_duration) & (col('trip_duration') <= max_duration))
```
<br>

#### Remove trips with a *distance* which is either too short or too long 
Similarly, I use the `min` and `max` functions to identify the shortest and longest trip distances in the dataset, then set `min_distance = 0.5` mph and `max_distance = 50` mph to filter out trips that are unrealistically short or long.

```python
# Define min and max distance for a trip
min_distance = 0.5 
max_distance = 50 

# Filter too long or too short trips
df_green = df_green.filter((col('trip_distance') >= min_distance) & (col('trip_distance') <= max_distance))
df_yellow = df_yellow.filter((col('trip_distance') >= min_distance) & (col('trip_distance') <= max_distance))
```
<br>

#### Remove trips with invalid number of passengers
According to NYC Taxi and Limousine Commission regulations, a taxi cab can carry no more than 5 passengers. Therefore, all trips having the number of passengers outside this range will be considered invalid records and need to be filtered out.

```python
df_green = df_green.filter((col('passenger_count') <= 5) & (col('passenger_count') > 0))
df_yellow = df_yellow.filter((col('passenger_count') <= 5) & (col('passenger_count') > 0))
```
<br>

The table below shows a summary of the number of removed records after each step:

<div align="center">

| Category                                           | Green      | Yellow      |
|---------------------------------------------------|------------|------------|
| Trips finishing before the starting time         | 838        | 81,067     |
| Trips where the pickup/dropoff datetime is outside of the range | 793        | 3,192      |
| Trips with negative speed                        | 75,129     | 645,276    |
| Trips with very high speed                       | 149,244    | 703,631    |
| Trips that are travelling too short or too long (duration wise) | 4,247,231  | 33,383,113 |
| Trips that are travelling too short or too long (distance wise) | 1,164,131  | 13,082,541 |
| Trips that have invalid number of passengers     | 2,735,145  | 25,344,371 |

</div>


After filtering, the **green** taxi dataset has 57,827,890 records, while the **yellow** taxi dataset has 589,812,060 records.<br>
<br>

## **Combine taxi datasets**
After the cleaning and transformation process, `df_green` and `df_yellow` are merged to create a complete dataset called `data`. Since the schemas of two datasets are not the same, some steps were handled to make 2 datasets consistent. 
  •	I dropped column `ehail_fee` from `df_green` because most of records in this dataset are null values.
  • Add columns with null values, and rename columns to ensure the columns of both datasets are exactly the same.
  •	A new column named taxi_color was added to both datasets before combining to separate green and yellow taxi cabs later.
  •	`.unionByName()` function was used to combine two datasets.  

```python
from pyspark.sql.functions import lit

# Drop columns
df_green = df_green.drop('ehail_fee')

# Add missing columns to 2 datasets
df_green = (df_green.withColumn('airport_fee', lit(None))
                    .withColumn('taxi_color',lit('green')))

df_yellow = (df_yellow.withColumn('trip_type', lit(None))
                      .withColumn('taxi_color',lit('yellow')))


# Rename lpep_pickup_datetime and lpep_dropoff_datetime in df_green 
df_green = (df_green.withColumnRenamed('lpep_pickup_datetime', 'tpep_pickup_datetime')
                    .withColumnRenamed('lpep_dropoff_datetime', 'tpep_dropoff_datetime')
                    .withColumnRenamed('lpep_pickup_timestamp', 'tpep_pickup_timestamp')
                    .withColumnRenamed('lpep_dropoff_timestamp', 'tpep_dropoff_timestamp'))

# Merge the dataframes
data = df_yellow.unionByName(df_green)
```
<br>

## **Combine taxi dataset with the location data**
The [location data]((taxi_zone_lookup.csv)) contains location ID and information about borough, zone, and service_zone. There are two locations in each trip, pick up location and drop off location. Therefore, we need to join the `taxi_zone_lookup` table twice:
- First join: to get location details for pickup (`PULocationID`)
- Second join: to get location details for dropoff (`DOLocationID`)

The problem is that if we join the same table twice without giving them different names, it would be confused. Therefore, it is necessary to use `alias()` to create 2 separate references to the `taxi_zone_lookup` table.

```python
# Alias the dataframes to differentiate between pickup and dropoff joins
lookup_pu = taxi_zone_lookup.alias('lookup_pu')
lookup_do = taxi_zone_lookup.alias('lookup_do')

# Left join for pickup location
df_with_pu = data.join(
                      lookup_pu,
                      data['PULocationID'] == lookup_pu['LocationID'],
                      how='left'
                      ).withColumnRenamed('Borough', 'PUBorough') \
                       .withColumnRenamed('Zone', 'PUZone') \
                       .withColumnRenamed('service_zone', 'PUservice_zone')

# Left join for drop-off location
df_final = df_with_pu.join(
                          lookup_do,
                          df_with_pu['DOLocationID'] == lookup_do['LocationID'],
                          how='left'
                          ).withColumnRenamed('Borough', 'DOBorough') \
                           .withColumnRenamed('Zone', 'DOZone') \
                           .withColumnRenamed('service_zone', 'DOservice_zone')

# Drop any unnecessary columns
df_final = df_final.drop(lookup_pu['LocationID']).drop(lookup_do['LocationID'])
```
<br>

## **Explore and analyze NYC taxi data**
To use Spark SQL queries, **temporary view** is required. `createOrReplaceTempView()` was used to create a `data_table` from the combined dataframe of previous step.
```sql
df_final.createOrReplaceTempView('data_table')
```
<br>

Query all records from the temporary view and store in a new dataframe `table`:
```sql
table = spark.sql('SELECT * FROM data_table')
```
<br>

Separate the dataframe into green and yellow:
```sql
green = table.filter(table.taxi_color == 'green')
yellow = table.filter(table.taxi_color == 'yellow')
```
<br>

Create temporary views and tables for both green and yellow data
```sql
green.createOrReplaceTempView('green_table')
green = spark.sql('SELECT * FROM green_table')

yellow.createOrReplaceTempView('yellow_table')
yellow = spark.sql('SELECT * FROM yellow_table')
```
<br>

1. For each **year and month**, select:
    - Total number of trips
    - Day of the week with the most trips
    - Hour of the day with the most trips
    - The average number of passengers
    - The average amount paid per trip
    - The average amount paid per passenger

```sql
SELECT
  DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS year_month,
  COUNT(*) AS total_trips,
  DATE_FORMAT(tpep_pickup_datetime, 'EEEE') AS day_of_week,
  HOUR(tpep_pickup_datetime) AS hour_of_day,
  AVG(passenger_count) AS avg_passenger,
  AVG(total_amount) AS avg_total_amount,
  AVG(total_amount / passenger_count) AS avg_amount_per_passenger
FROM data_table
GROUP BY year_month, day_of_week, hour_of_day
ORDER BY year_month, total_trips DESC
```
<br>

2. For each **taxi color**, select the average, median, minimum and maximum
    - Trip duration in minutes
    - Trip distance in km
    - Speed in km per hour

```sql
SELECT
    ROUND(AVG(trip_duration/60), 2) AS avg_trip_duration,
    ROUND(PERCENTILE(trip_duration/60,0.5),2) AS median_trip_duration,
    ROUND(MIN(trip_duration/60), 2) AS min_trip_duration,
    ROUND(MAX(trip_duration/60), 2) AS max_trip_duration,
    ROUND(AVG(trip_distance*1.60934), 2) AS avg_distance_km,
    ROUND(PERCENTILE(trip_distance*1.60934, 0.5), 2) AS median_distance_km,
    ROUND(MIN(trip_distance*1.60934), 2) AS min_distance_km,
    ROUND(MAX(trip_distance*1.60934), 2) AS max_distance_km,
    ROUND(AVG((trip_distance*1.60934) / (trip_duration/3600)), 2) AS avg_speed_kmph,
    ROUND(PERCENTILE((trip_distance*1.60934) / (trip_duration/3600), 0.5), 2) AS median_speed_kmph,
    ROUND(MIN((trip_distance*1.60934) / (trip_duration/3600)), 2) AS min_speed_kmph,
    ROUND(MAX((trip_distance*1.60934) / (trip_duration/3600)), 2) AS max_speed_kmph
FROM green_table
```
<br>

3. For each **taxi color**, each pair of pick up and drop off locations (use boroughs not the id), each month, each day of week and each hours, select:
    - Total number of trips
    - Average distance
    - Average amount paid per trip
    - Total amount paid

Green taxi
```sql
SELECT
    PUBorough,
    DOBorough,
    DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS year_month,
    DATE_FORMAT(tpep_pickup_datetime, 'EEEE') AS day_of_week,
    HOUR(tpep_pickup_datetime) AS hour_of_day,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(total_amount), 2) AS avg_amount_per_trip,
    ROUND(SUM(total_amount), 2) AS total_amount_paid
FROM green_table
GROUP BY PUBorough, DOBorough, year_month, day_of_week, hour_of_day
ORDER BY PUBorough, DOBorough, year_month, day_of_week, hour_of_day
```

Yellow taxi
```sql
SELECT
    PUBorough,
    DOBorough,
    DATE_FORMAT(tpep_pickup_datetime, 'yyyy-MM') AS year_month,
    DATE_FORMAT(tpep_pickup_datetime, 'EEEE') AS day_of_week,
    HOUR(tpep_pickup_datetime) AS hour_of_day,
    COUNT(*) AS total_trips,
    ROUND(AVG(trip_distance), 2) AS avg_distance,
    ROUND(AVG(total_amount), 2) AS avg_amount_per_trip,
    ROUND(SUM(total_amount), 2) AS total_amount_paid
FROM yellow_table
GROUP BY PUBorough, DOBorough, year_month, day_of_week, hour_of_day
ORDER BY PUBorough, DOBorough, year_month, day_of_week, hour_of_day
```
<br>

4. What was the percentage of trips where drivers received tips?
```sql
SELECT (COUNT(CASE WHEN tip_amount > 0 THEN 1 END) * 100 / COUNT(*)) AS percentage_trips_with_tips
FROM data_table;
```
<br>

5. For trips where the driver received tips, what was the percentage where the driver
received tips of at least $5?
```sql
SELECT (COUNT(CASE WHEN tip_amount >= 5 THEN 1 END) * 100 / COUNT(*)) AS percentage_trips_with_tips_at_least_5_dollars
FROM data_table;
```
<br>

6. Classify each trip into bins of durations:
  - Under 5 Mins
  - From 5 mins to 10 mins
  - From 10 mins to 20 mins
  - From 20 mins to 30 mins
  - From 30 mins to 60 mins
  - At least 60 mins

For each bins, calculate:
  - Average speed
  - Average distance

```sql
WITH trip_statistics AS (
    SELECT
        CASE
            WHEN trip_duration < 300 THEN 'Under 5 Mins'
            WHEN trip_duration >= 300 AND trip_duration < 600 THEN 'From 5 mins to 10 mins'
            WHEN trip_duration >= 600 AND trip_duration < 1200 THEN 'From 10 mins to 20 mins'
            WHEN trip_duration >= 1200 AND trip_duration < 1800 THEN 'From 20 mins to 30 mins'
            WHEN trip_duration >= 1800 AND trip_duration < 3600 THEN 'From 30 mins to 60 mins'
            ELSE 'At least 60 mins'
        END AS duration_bin,
        (trip_distance * 1.60934) / (trip_duration / 3600) AS speed,
        (trip_distance * 1.60934) / total_amount AS distance_per_dollar,
        tip_amount
    FROM data_table
    WHERE trip_duration > 0
)

SELECT
    duration_bin,
    ROUND((SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END) * 100 / COUNT(*)), 4) AS percentage_trips_with_tips,
    ROUND((SUM(CASE WHEN tip_amount >= 5 THEN 1 ELSE 0 END) * 100 / SUM(CASE WHEN tip_amount > 0 THEN 1 ELSE 0 END)), 4) AS percentage_trips_with_tips_at_least_5_dollars,
    ROUND(AVG(speed), 2) AS avg_speed_kmph,
    ROUND(AVG(distance_per_dollar), 2) AS avg_distance_per_dollar_kmpd
FROM trip_statistics
GROUP BY duration_bin;
```
<br>
Result:<br>
<p align="center">
  <img src="https://github.com/user-attachments/assets/0888634f-b71f-4b51-9fd4-3a9339295c4c" width="1000"/>
</p>
<br>

7. Which duration bin will you advise a taxi driver to target to maximise his income?
```sql
WITH trip_statistics AS (
    SELECT
        CASE
            WHEN trip_duration < 300 THEN 'Under 5 Mins'
            WHEN trip_duration >= 300 AND trip_duration < 600 THEN 'From 5 mins to 10 mins'
            WHEN trip_duration >= 600 AND trip_duration < 1200 THEN 'From 10 mins to 20 mins'
            WHEN trip_duration >= 1200 AND trip_duration < 1800 THEN 'From 20 mins to 30 mins'
            WHEN trip_duration >= 1800 AND trip_duration < 3600 THEN 'From 30 mins to 60 mins'
            ELSE 'At least 60 mins'
        END AS duration_bin,
        (trip_distance * 1.60934) / (trip_duration / 3600) AS speed,
        (trip_distance * 1.60934) / total_amount AS distance_per_dollar,
        tip_amount,
        total_amount, trip_duration
    FROM data_table
    WHERE trip_duration > 0
)
SELECT
    duration_bin,
    SUM(total_amount) AS total_income,
    SUM(trip_duration)/3600 AS total_trip_hours,
    SUM(total_amount) / (SUM(trip_duration)/3600) AS avg_income_per_hours
FROM trip_statistics
GROUP BY duration_bin;
```
<br>
Result:<br>

<p align="center">
  <img src="https://github.com/user-attachments/assets/d9ee434c-9a15-4e25-9667-98b858dd0f01" width="800" alt="question7" >
</p>

Based on the query results above, although trips within the `Under 5 Mins` duration bin generate less total income overall, they yield the highest earnings per hour, which is key for a driver aiming to maximize efficiency. Therefore, a taxi driver should target the `Under 5 Mins` trip.

