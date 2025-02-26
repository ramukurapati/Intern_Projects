from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from datetime import datetime
import os
import json

# Step 1: Initialize Spark session
spark = SparkSession.builder.appName("SilverToGoldProcessing").getOrCreate()

# Step 2: Load the configuration file
with open('config.json', 'r') as config_file:
    config = json.load(config_file)

silver_folder = config["paths"]["silver_folder"]
gold_folder = config["paths"]["gold_folder"]

# Step 3: Generate the path for today's date
current_date = datetime.now().strftime('%Y%m%d')

silver_input_paths = {
    "customers": os.path.join(silver_folder, "silver_customers", current_date, "customers"),
    "orders": os.path.join(silver_folder, "silver_orders", current_date, "orders"),
    "order_items": os.path.join(silver_folder, "silver_order_items", current_date, "order_items"),
    "products": os.path.join(silver_folder, "silver_products", current_date, "products"),
}

def load_csv(spark, path):
    wildcard_path = os.path.join(path, "*.csv")
    if os.path.exists(path):
        return spark.read.csv(wildcard_path, header=True, inferSchema=True)
    else:
        print(f"⚠ Warning: Directory not found, skipping {path}")
        return None

df_customers = load_csv(spark, silver_input_paths["customers"])
df_orders = load_csv(spark, silver_input_paths["orders"])
df_order_items = load_csv(spark, silver_input_paths["order_items"])
df_products = load_csv(spark, silver_input_paths["products"])

def check_column_exists(df, column_name):
    return df is not None and column_name in df.columns

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
            "o.order_status"  # Fixed column name
        ))
    print("Valid orders columns after join:", valid_orders.columns)
else:
    valid_orders = df_orders

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

def apply_business_rules(orders_df, order_items_df):
    if orders_df and order_items_df:
        active_orders = orders_df.filter((col("order_status") != "CANCELLED") & (col("total_amount") > 0))
        active_order_items = order_items_df.filter((col("quantity") > 0) & (col("unit_price") > 0)).join(active_orders, "order_id", "inner")
        return active_orders, active_order_items
    return orders_df, order_items_df

active_orders_df, active_order_items_df = apply_business_rules(valid_orders, valid_order_items)

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

def create_output_folders(base_path, subfolder, table_name):
    output_path = os.path.join(base_path, subfolder, current_date, table_name)
    os.makedirs(output_path, exist_ok=True)
    return output_path

gold_analytics_fact_sales = create_output_folders(gold_folder, "gold_analytics", "fact_sales")
gold_business_customers = create_output_folders(gold_folder, "gold_business_rules", "customers")
gold_business_orders = create_output_folders(gold_folder, "gold_business_rules", "orders")
gold_business_order_items = create_output_folders(gold_folder, "gold_business_rules", "order_items")
gold_business_products = create_output_folders(gold_folder, "gold_business_rules", "products")

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
