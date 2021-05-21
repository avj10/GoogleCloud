import re
import time
import sys

# A Spark Session is how we interact with Spark SQL to create Dataframes
from pyspark.sql import SparkSession

# This will help catch some PySpark errors
from py4j.protocol import Py4JJavaError

# starting time
start = time.time()

# Create a SparkSession under the name "reddit". Viewable via the Spark UI
spark = SparkSession.builder.appName("covid19").getOrCreate()

# Establish a set of years and months to iterate over
bucket_name = sys.argv[1]

# Keep track of all tables accessed via the job
tables_read = []

# In the form of <project-id>.<dataset>.<table>
table = f"bigdata-poc-281913.COVID_Dataset.covid_19_india"

# If the table doesn't exist simply continue and not
# log it into our "tables_read" list
try:
    df = spark.read.format('bigquery').option('table', table).load()
except Py4JJavaError:
    print(f"{table} does not exist. ")
    sys.exit(0)

print(f"Processing {table}.")

output_path = "gs://" + bucket_name + "/COVID_Dataset/covid_19_india"

df\
    .coalesce(1)\
    .write\
    .option("compression","gzip")\
    .mode('overwrite')\
    .parquet(output_path)

print(f"Google Cloud Stage Path : {output_path}")

end = time.time()

print(f"Runtime of the program is {(end - start)/60} minutes.")
