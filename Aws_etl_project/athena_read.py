import json
import boto3

config_path = "config.json"
with open(config_path, "r") as f:
    config = json.load(f)

athena_client = boto3.client("athena", region_name="us-east-1")

for table_info in config["tables"]:
    database = table_info["database"]
    table_name = table_info["table_name"]
    columns = table_info["columns"]

    s3_data_location = table_info[f"s3_{table_name.lower()}_location"]
    output_location = table_info[f"{table_name.lower()}_output_location"]

    columns_str = ",\n  ".join([f"{col['name']} {col['type']}" for col in columns])
    create_table_query = f"""
    CREATE EXTERNAL TABLE IF NOT EXISTS {database}.{table_name} (
      {columns_str}
    )
    STORED AS PARQUET
    LOCATION '{s3_data_location}';
    """

    print(f"Generated Query for {table_name}:\n", create_table_query)

    response = athena_client.start_query_execution(
        QueryString=create_table_query,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": output_location},
    )
    print(f"Athena query started for {table_name}. Execution ID:", response["QueryExecutionId"])
