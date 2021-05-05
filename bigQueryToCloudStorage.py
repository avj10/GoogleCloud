import re
import time
import sys

# A Spark Session is how we interact with Spark SQL to create Dataframes
from pyspark.sql import SparkSession

# PySpark function for replacing characters using a regex.
# Use this to remove newline characters.
from pyspark.sql.functions import regexp_replace, col

# Library for interacting with Google Cloud Storage
from google.cloud import storage

# This will help catch some PySpark errors
from py4j.protocol import Py4JJavaError

# starting time
start = time.time()

# Create a SparkSession under the name "reddit". Viewable via the Spark UI
spark = SparkSession.builder.appName("reddit").getOrCreate()

# Establish a set of years and months to iterate over
year = sys.argv[1]
month = sys.argv[2]
bucket_name = sys.argv[3]

# Establish a subreddit to process
subreddit = 'food'


# Keep track of all tables accessed via the job
tables_read = []

# In the form of <project-id>.<dataset>.<table>
table = f"fh-bigquery.reddit_posts.{year}_{month}"

# If the table doesn't exist simply continue and not
# log it into our "tables_read" list
try:
    df = spark.read.format('bigquery').option('table', table).load()
except Py4JJavaError:
    print(f"{table} does not exist. ")
    sys.exit(0)

print(f"Processing {table}.")

output_path = "gs://" + bucket_name + "/Reddit_posts/" + year + "/" + month

df \
.coalesce(1) \
.write \
.option("compression","gzip") \
.mode('overwrite') \
.parquet(output_path)

print(f"Done writing on path : {output_path}.")

end = time.time()

print(f"Runtime of the program is {(end - start)/60} minutes.")