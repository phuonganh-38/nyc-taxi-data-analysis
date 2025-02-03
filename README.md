# **New York City Taxi Data Analysis**
---

## **Introduction**
The project focuses on data processing and analysis using Databricks Spark, with the primary goal of leveraging Apache Spark to conduct a comprehensive analysis of a high-volume dataset. It aims to analyse a large dataset from New York City taxi trips by loading, transforming, and performing detailed analysis to derive valuable insights and predictions. Throughout the project, Databricks is utilised as a core platform to process and manipulate data, with the dataset stored in Microsoft Azure Blob Storage. Additionally, various tools and technologies are employed to process and transform data, including Python, PySpark, and Spark SQL. After being ingested from Azure, the dataset undergoes extensive cleaning to remove unrealistic records, then be explored using Spark SQL to extract insights into taxi operations, trip patterns, and passenger behaviour. By the end of the project, Spark ML pipelines will be used to build and train predictive models, with performance evaluated against a baseline to ensure accuracy in predicting trip totals.


## **Key objectives**
The primary goal of the project is to conduct a comprehensive analysis of a large dataset using Apache Spark, with a focus on data ingestion, transformation, machine learning model development for predicting profound findings.


## **Project workflow**
Dataset acquisition → Set up Azure Blob storage (create storage account and container) → Upload dataset to Azure → Ingest data to Databricks → Explore and manipulate data → Develop Machine Learning models


## **Dataset**
The dataset for this project is provided by the New York City Taxi and Limousine Commission (TLC), a company which has been responsible for managing license and regulating New York City’s taxis since 1971. The dataset comprises
16 parquet files,representing taxi data from 2015 to 2022 of two types of taxis cabs: yellow and green taxis, and a [location data](taxi_zone_lookup.csv). The dataset for taxi cabs can be downloaded from the following links:

### **Yellow taxi**
- [yellow_taxi_2015.parquet](https://drive.google.com/file/d/1owWyJDNTWyLT0ln2iK5ulkmSYvXZ7qkf/view)
- [yellow_taxi_2016.parquet](https://drive.google.com/file/d/1OdIcvpyFH1YXn9SNHc8YEuVFAYQpxCUw/view)
- [yellow_taxi_2017.parquet](https://drive.google.com/file/d/1rtEhtit_2rKvWgutXNIpSWPk3vuE6q8r/view)
- [yellow_taxi_2018.parquet](https://drive.google.com/file/d/1073SHSIkWcSESNZoU0JHXudRlSzJXPM9/view)
- [yellow_taxi_2019.parquet](https://drive.google.com/file/d/144mphzh2a6qerjLvCwwO_QHDsXTdNAJ3/view)
- [yellow_taxi_2020.parquet](https://drive.google.com/file/d/1kB5Bnx1TAXMq_revh1fyUU5RYOMdmIh4/view)
- [yellow_taxi_2021.parquet](https://drive.google.com/file/d/1eTs-ID9A3ZgYy0BotrEKwh9ThRAt8dfu/view)
- [yellow_taxi_2022.parquet](https://drive.google.com/file/d/1QdBDxHQzffBZ26T3j6Uhk1eJ8EmF0bCN/view)

### **Green taxi**
- [green_taxi_2015.parquet](https://drive.google.com/file/d/137oXWkqBOQcxmgHynPv6Wh_fHUqN40n3/view)
- [green_taxi_2016.parquet](https://drive.google.com/file/d/1s0drAVqulJ_hE4RRqMSNJWyQGF6RKAtA/view)
- [green_taxi_2017.parquet](https://drive.google.com/file/d/1-VpjWArKPEdjzlTZxI7aPwd8UsVfc2bL/view)
- [green_taxi_2018.parquet](https://drive.google.com/file/d/1jDn7qjFZ3-nrn4iOdFMh_p0W21esiIHn/view)
- [green_taxi_2019.parquet](https://drive.google.com/file/d/1BpjTq89EAhb6m-ICcZMEpTISw6jTHmio/view)
- [green_taxi_2020.parquet](https://drive.google.com/file/d/1umIMHrqaqagZYqvLLf-OzidnDPwxIY5j/view)
- [green_taxi_2021.parquet](https://drive.google.com/file/d/1ISKrR97II-zWR7f2_boFcyfsNgsj8K1Y/view)
- [green_taxi_2022.parquet](https://drive.google.com/file/d/1ysXV_4hB3Ex43k1HOvCi8RCT1k7GZANj/view)


## **Tools used**
- Microsoft Azure
- Databricks
- Apache Spark
- Python
- Pyspark
- SparkSQL


## **Features**
- **Large-scale data handling**: Efficient processing of a massive dataset with approximately 800 million records, leveraging the distributed computing power of Apache Spark on Databricks
- **Integration with Azure Blob Storage**: Seamless integration with Azure Blob Storage as the primary storage solution for managing.
- **End-to-End workflow in Databricks**
- **Data cleaning and transformation**: Comprehensive data cleaning and transformation pipelines implemented using PySpark, ensuring high-quality data for analysis and modeling
- **Predictive Modeling**: Develop Linear Regression and Random Forest with the goal of helping stakeholders in understanding fare dynamics and optimizing pricing strategies


## **Data Cleaning**

#### Remove trips finishing before starting time

```python
df_green = df_green.filter(df_green['lpep_dropoff_datetime'] >= df_green['lpep_pickup_datetime'])

df_yellow = df_yellow.filter(df_yellow['tpep_dropoff_datetime'] >= df_yellow['tpep_pickup_datetime'])
```

#### Remove trips where the pickup/dropoff datetime is outside of the range
Firstly, we need to define the valid datetime range:
```python
valid_start_date = '2015-01-01T00:00:00.000+00:00'
valid_end_date = '2022-12-31T23:59:59.999+00:00'
```

Remove trips where the time is outside of the range
```python
# df_green 
df_green = df_green.filter((col('lpep_pickup_timestamp') >= valid_start_date) & (col('lpep_pickup_timestamp') <= valid_end_date) &
                           (col('lpep_dropoff_timestamp') >= valid_start_date) & (col('lpep_dropoff_timestamp') <= valid_end_date))

# df_yellow
df_yellow = df_yellow.filter((col('tpep_pickup_timestamp') >= valid_start_date) & (col('tpep_pickup_timestamp') <= valid_end_date) &
                             (col('tpep_dropoff_timestamp') >= valid_start_date) & (col('tpep_dropoff_timestamp') <= valid_end_date))
```

