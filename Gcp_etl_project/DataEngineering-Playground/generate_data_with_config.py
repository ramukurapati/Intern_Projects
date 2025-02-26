import csv
import random
import datetime
import sys
import json
import gcsfs

def load_config(config_file):
    """Loads configuration from a JSON file on GCS."""
    fs = gcsfs.GCSFileSystem()
    try:
        with fs.open(config_file, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"❌ Error: Could not read '{config_file}'. Using default config. Error: {e}")
        return {
            "paths": {
                "output_folder": "gs://intern-medallion-architecture/DataEngineering-Playground",
                "tracker_file": "gs://intern-medallion-architecture/DataEngineering-Playground/id_tracker.json"
            },
            "data_settings": {
                "num_rows_per_table": 5000
            }
        }

def load_id_tracker(tracker_path):
    """Load the last used IDs from a JSON file on GCS."""
    fs = gcsfs.GCSFileSystem()
    try:
        with fs.open(tracker_path, 'r') as f:
            return json.load(f)
    except:
        return {"customers": 10000, "products": 10000, "orders": 10000, "order_items": 10000}

def save_id_tracker(tracker, tracker_path):
    """Save the updated IDs to a JSON file on GCS."""
    fs = gcsfs.GCSFileSystem()
    with fs.open(tracker_path, 'w') as f:
        json.dump(tracker, f, indent=2)

def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date."""
    start_ts = int(start_date.timestamp())
    end_ts = int(end_date.timestamp())
    rand_ts = random.randint(start_ts, end_ts)
    return datetime.date.fromtimestamp(rand_ts)

def generate_customers(num_rows, start_id):
    """Generate customer data."""
    states = ["NY", "CA", "TX", "IL", "FL", "WA", "MA", "OH", "GA", "NC"]
    rows = [[i, f"Customer {i}", f"customer{i}@example.com",
             f"{random.randint(1,9999)} Main St", random.choice(states)]
            for i in range(start_id, start_id + num_rows)]
    return rows

def generate_products(num_rows, start_id):
    """Generate product data."""
    categories = ["Electronics", "Wearables", "Home Decor", "Kitchen", "Sports"]
    rows = [[i, f"Product {i}", random.choice(categories), round(random.uniform(5.0, 1000.0), 2)]
            for i in range(start_id, start_id + num_rows)]
    return rows

def generate_orders(num_rows, start_id, max_customer_id, order_date=None):
    """Generate order data."""
    order_statuses = ["SHIPPED", "PENDING", "CANCELLED", "DELIVERED"]
    start_range, end_range = datetime.date(2024, 1, 1), datetime.date(2025, 1, 1)

    rows = [[i, order_date or random_date(start_range, end_range), random.randint(1, max_customer_id),
             round(random.uniform(0.0, 5000.0), 2), random.choice(order_statuses)]
            for i in range(start_id, start_id + num_rows)]
    return rows

def generate_order_items(num_rows, start_id, max_order_id, max_product_id):
    """Generate order item data."""
    rows = [[i, random.randint(1, max_order_id), random.randint(1, max_product_id),
             random.randint(1, 10), round(random.uniform(5.0, 500.0), 2)]
            for i in range(start_id, start_id + num_rows)]
    return rows

def write_csv(file_name, header, rows):
    """Write CSV data to Google Cloud Storage (GCS)."""
    fs = gcsfs.GCSFileSystem()
    with fs.open(file_name, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rows)

def main():
    """
    Main execution function.
    Accepts:
    - config_file (required): Path to the config.json file.
    - run_date (optional): Date in YYYY-MM-DD format. Defaults to today if not provided.
    """
    if len(sys.argv) < 2:
        print("❌ Error: Missing required argument: config.json path.")
        sys.exit(1)

    # ✅ First argument: Config file path
    config_file = sys.argv[1]
    config = load_config(config_file)

    output_folder = config["paths"]["output_folder"]
    tracker_file = config["paths"]["tracker_file"]
    num_rows = config["data_settings"]["num_rows_per_table"]

    # ✅ Second argument (optional): Run date in YYYY-MM-DD format
    if len(sys.argv) > 2:
        try:
            run_date = datetime.datetime.strptime(sys.argv[2], "%Y-%m-%d").date()
        except ValueError:
            print("❌ Invalid date format. Use YYYY-MM-DD.")
            sys.exit(1)
    else:
        run_date = datetime.date.today()  # Default to today's date

    date_str = run_date.strftime("%Y%m%d")
    file_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

    # Load tracker
    tracker = load_id_tracker(tracker_file)

    # Generate data
    c_start_id = tracker["customers"] + 1
    p_start_id = tracker["products"] + 1
    o_start_id = tracker["orders"] + 1
    oi_start_id = tracker["order_items"] + 1

    customers_data = generate_customers(num_rows, c_start_id)
    products_data = generate_products(num_rows, p_start_id)
    orders_data = generate_orders(num_rows, o_start_id, tracker["customers"], run_date)
    order_items_data = generate_order_items(num_rows, oi_start_id, tracker["orders"], tracker["products"])

    # Update tracker
    tracker.update({
        "customers": c_start_id + num_rows - 1,
        "products": p_start_id + num_rows - 1,
        "orders": o_start_id + num_rows - 1,
        "order_items": oi_start_id + num_rows - 1
    })
    save_id_tracker(tracker, tracker_file)

    # Write files to GCS
    write_csv(f"{output_folder}/raw_customers/{date_str}/raw_customers_{file_timestamp}.csv",
              ["customer_id", "full_name", "email", "address", "state"], customers_data)

    write_csv(f"{output_folder}/raw_products/{date_str}/raw_products_{file_timestamp}.csv",
              ["product_id", "product_name", "category", "price"], products_data)

    write_csv(f"{output_folder}/raw_orders/{date_str}/raw_orders_{file_timestamp}.csv",
              ["order_id", "order_date", "customer_id", "total_amount", "order_status"], orders_data)

    write_csv(f"{output_folder}/raw_order_items/{date_str}/raw_order_items_{file_timestamp}.csv",
              ["order_item_id", "order_id", "product_id", "quantity", "unit_price"], order_items_data)

    print("\nUpdated ID tracker:", tracker_file)

if __name__ == "__main__":
    main()
