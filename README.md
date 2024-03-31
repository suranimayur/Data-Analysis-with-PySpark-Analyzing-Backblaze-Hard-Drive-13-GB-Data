# Data-Analysis-with-PySpark-Analyzing-Backblaze-Hard-Drive-13-GB-Data

Our project focuses on leveraging PySpark, a powerful distributed computing framework, to analyze Backblaze hard drive data. Backblaze, a cloud storage provider, regularly publishes detailed statistics on the performance and reliability of its hard drives. By harnessing PySpark's capabilities, we aim to uncover valuable insights from this rich dataset, enabling better decision-making for storage infrastructure and reliability engineering.

## Key Features:

Data Loading: We provide code snippets to efficiently load and preprocess the Backblaze hard drive data from CSV files spanning multiple quarters of the year 2019.
Data Analysis: Our project showcases the implementation of PySpark functions for data exploration, aggregation, and analysis. Specifically, we demonstrate how to identify the most reliable hard drives based on capacity and failure rates using PySpark SQL and DataFrame operations.
Scalability: PySpark's distributed computing capabilities enable seamless scalability for processing large-scale datasets. We highlight the scalability and performance benefits of PySpark in handling extensive volumes of hard drive data.

## Why PySpark:

PySpark offers a user-friendly interface for big data processing and analytics, making it an ideal choice for handling diverse datasets like Backblaze hard drive data. With PySpark, users can harness the power of distributed computing while leveraging familiar SQL-like syntax and DataFrame operations for efficient data manipulation and analysis.


## Code snipper

from functools import partial, reduce
import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession

spark = SparkSession.builder.appName("Backblaze_Data_Analysis").getOrCreate()

data_directories = [
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/code/Ch07/backblaze/data_Q1_2019/*.csv',
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/code/Ch07/backblaze/data_Q2_2019/*.csv',
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/code/Ch07/backblaze/data_Q3_2019/*.csv',
    '/home/surani/python_projects/DataAnalysisWithPythonAndPySpark/code/Ch07/backblaze/data_Q4_2019/*.csv',
]

data = spark.read.csv(data_directories, inferSchema=True, header=True)

data.printSchema()
data.show(5)

scolumns = [s for s in data.columns]
common_columns = [s for s in data.columns]

assert set(["model", "capacity_bytes", "date", "failure"]).issubset(set(scolumns))

full_data = data

full_data.printSchema()
full_data.show(5)

def most_reliable_drive_for_capacity(data, capacity_GB=2048, precision=0.25, top_n=3):
    """
    Return the most reliable drives for a given capacity.

    Args:
        data (DataFrame): The DataFrame containing drive data.
        capacity_GB (int, optional): The capacity in GB. Defaults to 2048.
        precision (float, optional): The precision as a decimal number. Defaults to 0.25.
        top_n (int, optional): The number of drives to return. Defaults to 3.
    
    Returns:
        DataFrame: The DataFrame containing the most reliable drives for the given capacity.
    """
    capacity_min = capacity_GB / (1 + precision)
    capacity_max = capacity_GB * (1 + precision)

    answer = (
        data.where(f"capacity_GB between {capacity_min} and {capacity_max}")
            .orderBy("failure_rate", ascending=[True, False])
            .limit(top_n)
    )

    return answer.show()

most_reliable_drive_for_capacity(full_data, capacity_GB=1124, precision=0.25, top_n=3)



## Function Snippets:
most_reliable_drive_for_capacity Function:

def most_reliable_drive_for_capacity(data, capacity_GB=2048, precision=0.25, top_n=3):
    """
    Return the most reliable drives for a given capacity.

    Args:
        data (DataFrame): The DataFrame containing drive data.
        capacity_GB (int, optional): The capacity in GB. Defaults to 2048.
        precision (float, optional): The precision as a decimal number. Defaults to 0.25.
        top_n (int, optional): The number of drives to return. Defaults to 3.
    
    Returns:
        DataFrame: The DataFrame containing the most reliable drives for the given capacity.
    """
    capacity_min = capacity_GB / (1 + precision)
    capacity_max = capacity_GB * (1 + precision)

    answer = (
        data.where(f"capacity_GB between {capacity_min} and {capacity_max}")
            .orderBy("failure_rate", ascending=[True, False])
            .limit(top_n)
    )

    return answer.show()


## Final Summary:

**Project Overview:** This project analyzes Backblaze hard drive data using PySpark, a distributed computing framework for big data processing.
**Data Loading:** The data is loaded from CSV files located in different directories representing quarterly data for the year 2019.
**Data Preparation:** Initial data exploration and schema inspection are performed to ensure data quality and consistency.
**Data Analysis:** A function named most_reliable_drive_for_capacity is implemented to identify the most reliable hard drives based on capacity and failure rates.


## Conclusion: 

PySpark provides a powerful platform for analyzing large-scale datasets efficiently. By leveraging its SQL API and DataFrame operations, insights can be gained from complex data structures such as Backblaze hard drive data. The most_reliable_drive_for_capacity function demonstrates the flexibility and scalability of PySpark for real-world data analysis tasks.





