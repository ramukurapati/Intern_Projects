from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os
import json
import gcsfs

# Step 1: Initialize a Spark session
spark = SparkSession.builder.appName("SilverLayerValidation").getOrCreate()

# Step 2: Read the configuration file from GCS
gcs = gcsfs.GCSFileSystem(project="your-gcp-project-id")
config_path = "gs://intern-medallion-architecture/DataEngineering-Playground/config.json"

# Load the configuration JSON file
with gcs.open(config_path, 'r') as config_file:
    config = json.load(config_file)

paths = config["paths"]["bronze_folder"]

# Step 3: Generate the path for today's date
current_date = datetime.now().strftime('%Y%m%d')
nested_path_customers = os.path.join(paths, "bronze_customers", current_date, "customers")
nested_path_orders = os.path.join(paths, "bronze_orders", current_date, "orders")
nested_path_order_items = os.path.join(paths, "bronze_order_items", current_date, "order_items")
nested_path_products = os.path.join(paths, "bronze_products", current_date, "products")

# Step 4: Create the wildcard paths for each table
csv_file_path_customers = os.path.join(nested_path_customers, "*.csv")
csv_file_path_orders = os.path.join(nested_path_orders, "*.csv")
csv_file_path_order_items = os.path.join(nested_path_order_items, "*.csv")
csv_file_path_products = os.path.join(nested_path_products, "*.csv")

# Step 5: Load the CSV files into DataFrames
df_customers = spark.read.csv(csv_file_path_customers, header=True, inferSchema=True)
df_orders = spark.read.csv(csv_file_path_orders, header=True, inferSchema=True)
df_order_items = spark.read.csv(csv_file_path_order_items, header=True, inferSchema=True)
df_products = spark.read.csv(csv_file_path_products, header=True, inferSchema=True)

# Step 6: Clean data by removing invalid records and applying basic validation
df_orders_cleaned = df_orders.filter(col("total_amount") > 0)
df_products_cleaned = df_products.filter(col("price") > 0)
df_order_items_cleaned = df_order_items.filter((col("quantity") > 0) & (col("unit_price") > 0))
df_customers_cleaned = df_customers  # No specific cleaning rules provided for customers

# Step 7: Check for presence of required columns
def check_column_exists(df, column_name):
    return column_name in df.columns

# Check 1: Validate presence of customer_id in orders and customers
if not (check_column_exists(df_orders, "customer_id") and check_column_exists(df_customers, "customer_id")):
    raise ValueError("Required column 'customer_id' missing in orders or customers table.")

# Check 2: Validate presence of order_id in order_items and orders
if not (check_column_exists(df_order_items, "order_id") and check_column_exists(df_orders, "order_id")):
    raise ValueError("Required column 'order_id' missing in order_items or orders table.")

# Step 8: Remove duplicate columns if they exist (preserve timestamp)
def remove_duplicate_columns(df, columns_to_remove):
    """ Drops columns if they exist in the DataFrame """
    for column_name in columns_to_remove:
        if column_name in df.columns:
            df = df.drop(column_name)
    return df

# Remove only source_system column before writing, keep timestamp
columns_to_remove = ["source_system"]  # Removed 'timestamp' from the list
df_orders_cleaned = remove_duplicate_columns(df_orders_cleaned, columns_to_remove)
df_products_cleaned = remove_duplicate_columns(df_products_cleaned, columns_to_remove)
df_customers_cleaned = remove_duplicate_columns(df_customers_cleaned, columns_to_remove)
df_order_items_cleaned = remove_duplicate_columns(df_order_items_cleaned, columns_to_remove)

# Step 9: Define the silver layer paths (WITHOUT .csv EXTENSION)
silver_folder = config["paths"]["silver_folder"]
silver_layer_orders_path = os.path.join(silver_folder, "silver_orders", current_date, "orders")
silver_layer_order_items_path = os.path.join(silver_folder, "silver_order_items", current_date, "order_items")
silver_layer_products_path = os.path.join(silver_folder, "silver_products", current_date, "products")
silver_layer_customers_path = os.path.join(silver_folder, "silver_customers", current_date, "customers")

# Step 10: Write cleaned original data to the silver layer using coalesce(1)
df_orders_cleaned.coalesce(1).write.mode('overwrite').option("header", "true").csv(silver_layer_orders_path)
df_order_items_cleaned.coalesce(1).write.mode('overwrite').option("header", "true").csv(silver_layer_order_items_path)
df_products_cleaned.coalesce(1).write.mode('overwrite').option("header", "true").csv(silver_layer_products_path)
df_customers_cleaned.coalesce(1).write.mode('overwrite').option("header", "true").csv(silver_layer_customers_path)

print("Silver data written successfully.")
spark.stop()
