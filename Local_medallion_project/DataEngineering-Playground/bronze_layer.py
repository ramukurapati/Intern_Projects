import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime
import os

# Load the config file and extract the base path and relative paths
with open("config.json", "r") as config_file:
    config = json.load(config_file)

# Fetch relative paths from the config
paths = config["paths"]

# Initialize Spark session
spark = SparkSession.builder.appName("ReadCSVFiles").getOrCreate()

# Get the current date in 'yyyymmdd' format
current_date = datetime.now().strftime('%Y%m%d')

# Function to read CSV, transform, and write the DataFrame
def process_and_write_data(raw_folder, bronze_folder, file_name, source_system):
    # Dynamically construct the paths
    raw_path = os.path.join(paths[raw_folder], current_date, "*.csv")
    bronze_folder_path = os.path.join(paths["bronze_folder"], bronze_folder, current_date)

    # Create the folder if it doesn't exist
    os.makedirs(bronze_folder_path, exist_ok=True)

    # Read the CSV file into a DataFrame
    df = spark.read.csv(raw_path, header=True, inferSchema=True)

    # Add timestamp and source_system columns
    df = df.withColumn("timestamp", current_timestamp()).withColumn("source_system", lit(source_system))

    # Count the number of rows for reporting
    row_count = df.count()
    print(f"Number of rows in {source_system}: {row_count}")

    # Construct the output file path
    file_path = os.path.join(bronze_folder_path, file_name)

    # Write the DataFrame to the bronze folder as CSV
    df.coalesce(1).write.csv(file_path, header=True, mode='overwrite')

    # Log where the CSV file was written
    print(f"CSV file written to: {file_path}")

# Process and write data for each dataset
# Customer data
process_and_write_data(
    raw_folder="raw_customers",
    bronze_folder="bronze_customers",
    file_name="customers",
    source_system="Customer Data"
)

# Order items data
process_and_write_data(
    raw_folder="raw_order_items",
    bronze_folder="bronze_order_items",
    file_name="order_items",
    source_system="Order Items"
)

# Orders data
process_and_write_data(
    raw_folder="raw_orders",
    bronze_folder="bronze_orders",
    file_name="orders",
    source_system="Orders"
)

# Products data
process_and_write_data(
    raw_folder="raw_products",
    bronze_folder="bronze_products",
    file_name="products",
    source_system="Products"
)
print("Bronze data written successfully.")