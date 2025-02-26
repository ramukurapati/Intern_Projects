# README: AWS Athena External Table Creation with Boto3

## Overview
This script automates the creation of **Amazon Athena** external tables using **Parquet files** stored in **Amazon S3**. The script reads table definitions from a **config.json** file and executes **Athena queries** via **Boto3**.

## Prerequisites
Before running this script, ensure you have:
1. **AWS Credentials** configured via `aws configure` or environment variables.
2. **Boto3** installed: `pip install boto3`
3. **A valid `config.json` file** with database and table configurations.
4. **Parquet files** stored in S3 for Athena to query.

## Configuration File (`config.json`)
The `config.json` file contains table definitions, including:
- **Database name** (existing in Athena)
- **Table name**
- **Column definitions** (name and data type)
- **S3 location** for the Parquet files
- **S3 output location** for query results

Example:
```json
{
  "tables": [
    {
      "database": "internproject",
      "table_name": "housing_data",
      "columns": [
        {"name": "price", "type": "bigint"},
        {"name": "area", "type": "bigint"}
      ],
      "s3_housing_data_location": "s3://parquetfiles-input-load/parquetfiles/housing-data/",
      "housing_data_output_location": "s3://athenaquery-output-files/athenaquery-output/athena-housing-output/"
    }
  ]
}
```

## How It Works
1. **Reads** table configurations from `config.json`
2. **Builds** an Athena `CREATE EXTERNAL TABLE` SQL query dynamically.
3. **Executes** the query using **Boto3 Athena client**.
4. **Prints** the generated SQL query and execution status.

## Code Execution
### Running the Script
Execute the script with:
```bash
python script.py
```

### Expected Output
For each table, the script will:
1. Print the **generated SQL query**.
2. Print the **Query Execution ID**.

Example:
```
Generated Query for housing_data:
CREATE EXTERNAL TABLE IF NOT EXISTS internproject.housing_data (
  price bigint,
  area bigint
)
STORED AS PARQUET
LOCATION 's3://parquetfiles-input-load/parquetfiles/housing-data/';

Athena query started for housing_data. Execution ID: abc123xyz
```

## Error Handling
- **Missing or incorrect S3 path**: Ensure Parquet files exist in the specified S3 bucket.
- **Permission errors**: Verify IAM roles have correct `Athena` and `S3` permissions.
- **Invalid SQL syntax**: Ensure column definitions are correctly formatted in `config.json`.

## Conclusion
This script simplifies Athena table creation by automating query execution based on JSON configurations. Modify the `config.json` file to include additional tables as needed.

For troubleshooting, check Athena query logs in the **AWS Console > Athena > Query History**.

