from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import json
from google.cloud import storage

# Step 1: Initialize Spark session with GCS connector
spark = SparkSession.builder \
    .appName("SilverToGoldProcessing") \
    .config("spark.jars.packages", "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.22") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .getOrCreate()

# Step 2: Load the configuration file from GCS
gcs_config_path = 'gs://intern-medallion-architecture/DataEngineering-Playground/config.json'

def load_config_from_gcs(gcs_path):
    client = storage.Client()
    bucket_name, blob_name = gcs_path.replace('gs://', '').split('/', 1)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    config_data = blob.download_as_text()
    return json.loads(config_data)

config = load_config_from_gcs(gcs_config_path)

# Step 3: Extract folder paths from the config
silver_folder = config["paths"]["silver_folder"]
gold_folder = config["paths"]["gold_folder"]

# Step 4: Generate the path for today's date
current_date = datetime.now().strftime('%Y%m%d')

silver_input_paths = {
    "customers": f"{silver_folder}/silver_customers/{current_date}/customers",
    "orders": f"{silver_folder}/silver_orders/{current_date}/orders",
    "order_items": f"{silver_folder}/silver_order_items/{current_date}/order_items",
    "products": f"{silver_folder}/silver_products/{current_date}/products",
}

# Step 5: Function to load CSV data from GCS
def load_csv(spark, path):
    try:
        # Use wildcard to read all CSV files in the directory
        return spark.read.option("header", "true").option("inferSchema", "true").csv(f"{path}/*.csv")
    except Exception as e:
        print(f"⚠ Warning: Could not load data from {path}. Error: {str(e)}")
        return None

# Load CSV data
df_customers = load_csv(spark, silver_input_paths["customers"])
df_orders = load_csv(spark, silver_input_paths["orders"])
df_order_items = load_csv(spark, silver_input_paths["order_items"])
df_products = load_csv(spark, silver_input_paths["products"])

# Step 6: Column existence check
def check_column_exists(df, column_name):
    return df is not None and column_name in df.columns

# Step 7: Join orders with customers
if df_customers and df_orders:
    print("Orders columns before join:", df_orders.columns)
    print("Customers columns before join:", df_customers.columns)
    valid_orders = (df_orders.alias("o")
        .join(df_customers.alias("c"), df_orders.customer_id == df_customers.customer_id, "inner")
        .select(
            "o.order_id",
            "o.customer_id",
            "o.order_date",
            "o.total_amount",
            "o.order_status"
        ))
    print("Valid orders columns after join:", valid_orders.columns)
else:
    valid_orders = df_orders

# Step 8: Join order items with valid orders
if df_order_items and valid_orders:
    print("Order items columns before join:", df_order_items.columns)
    print("Valid orders columns before join:", valid_orders.columns)
    valid_order_items = (df_order_items.alias("oi")
        .join(valid_orders.alias("o"), df_order_items.order_id == valid_orders.order_id, "inner")
        .select(
            "oi.order_id",
            "oi.product_id",
            "oi.quantity",
            "oi.unit_price"
        ))
    print("Valid order items columns after join:", valid_order_items.columns)
else:
    valid_order_items = df_order_items

# Step 9: Create Fact Sales DataFrame
def create_fact_sales(orders_df, order_items_df, products_df):
    if all(df is not None for df in [orders_df, order_items_df, products_df]):
        fact_sales = (orders_df.alias("o")
            .join(order_items_df.alias("oi"), "order_id", "inner")
            .join(products_df.alias("p"), order_items_df.product_id == products_df.product_id, "inner")
            .select(
                "o.order_id",
                "o.customer_id",
                "o.order_date",
                "oi.product_id",
                "p.product_name",
                "oi.quantity",
                "oi.unit_price",
                (order_items_df.quantity * order_items_df.unit_price).alias("line_total"),
                "o.total_amount"
            ))
        print("Fact sales columns:", fact_sales.columns)
        return fact_sales
    return None

fact_sales_df = create_fact_sales(valid_orders, valid_order_items, df_products)

# Step 10: Apply business rules to the data
def apply_business_rules(orders_df, order_items_df):
    if orders_df and order_items_df:
        active_orders = orders_df.filter((col("order_status") != "CANCELLED") & (col("total_amount") > 0))
        active_order_items = order_items_df.filter((col("quantity") > 0) & (col("unit_price") > 0))\
            .join(active_orders, "order_id", "inner")
        return active_orders, active_order_items
    return orders_df, order_items_df

active_orders_df, active_order_items_df = apply_business_rules(valid_orders, valid_order_items)

# Step 11: Remove unwanted columns
def remove_duplicate_columns(df, columns_to_remove):
    if df:
        for column_name in columns_to_remove:
            if column_name in df.columns:
                df = df.drop(column_name)
    return df

columns_to_remove = ["source_system", "timestamp"]
fact_sales_df = remove_duplicate_columns(fact_sales_df, columns_to_remove)
active_orders_df = remove_duplicate_columns(active_orders_df, columns_to_remove)
active_order_items_df = remove_duplicate_columns(active_order_items_df, columns_to_remove)
df_customers = remove_duplicate_columns(df_customers, columns_to_remove)
df_products = remove_duplicate_columns(df_products, columns_to_remove)

# Step 12: Define output folder structure
gold_analytics_fact_sales = f"{gold_folder}/gold_analytics/{current_date}/fact_sales"
gold_business_customers = f"{gold_folder}/gold_business_rules/{current_date}/customers"
gold_business_orders = f"{gold_folder}/gold_business_rules/{current_date}/orders"
gold_business_order_items = f"{gold_folder}/gold_business_rules/{current_date}/order_items"
gold_business_products = f"{gold_folder}/gold_business_rules/{current_date}/products"

# Step 13: Write to gold layer
def write_to_gold(df, path, name):
    if df:
        df.coalesce(1).write.mode('overwrite').option("header", "true").csv(path)
        print(f"{name} written to Gold Layer: {path}")
    else:
        print(f"Skipping {name}, no valid data available.")

write_to_gold(fact_sales_df, gold_analytics_fact_sales, "Fact Sales")
write_to_gold(df_customers, gold_business_customers, "Customers")
write_to_gold(active_orders_df, gold_business_orders, "Orders")
write_to_gold(active_order_items_df, gold_business_order_items, "Order Items")
write_to_gold(df_products, gold_business_products, "Products")

print("Gold Layer processing completed successfully.")
spark.stop()