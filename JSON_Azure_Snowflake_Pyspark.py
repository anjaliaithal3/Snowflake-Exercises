
from pyspark.sql import SparkSession
import snowflake.connector
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import col

def create_spark_session(appName):
    #Creates a Spark session with Snowflake connector
    spark = SparkSession.builder \
        .appName(appName) \
        .config("spark.jars.packages",
                "C:/Spark/spark-3.5.5-bin-hadoop3/jars/spark-snowflake_2.12-2.16.0.jar,"
                "C:/Spark/spark-3.5.5-bin-hadoop3/jars/snowflake-jdbc-3.13.14.jar") \
        .getOrCreate()

    print("Spark session started successfully!")
    return spark

# Snowflake Credentials
SNOWFLAKE_OPTIONS = {
    "sfURL": "https://zndegxs-vt29033.snowflakecomputing.com",
    "sfDatabase": "AZURE_DATA",
    "sfSchema": "PYSPARK_JSON_SCHEMA",
    "sfWarehouse": "COMPUTE_WH",
    "sfRole": "ACCOUNTADMIN",
    "sfUser": "ANJALIAITHAL",
    "sfPassword": "Jumanji1234567"
}

# Azure Blob Storage Credentials
storage_account_name = "snowflakeevdata"
sas_token = "sp=r&st=2025-03-29T10:52:36Z&se=2025-04-03T18:52:36Z&spr=https&sv=2024-11-04&sr=c&sig=LQ9jQB2CMUtpiEom8Ec3GFRwTQ2wydlhCbdV%2Br9atLQ%3D"
container_name = "snowflake-ev-demo"
blob_path = "ElectricVehiclePopulationData.json"

def read_json_from_azure():
    #Reads JSON file from Azure Blob Storage
    spark.conf.set("fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    spark.conf.set(
        f"fs.azure.sas.{container_name}.{storage_account_name}.blob.core.windows.net",
        sas_token
    )
    file_path = f"wasbs://{container_name}@{storage_account_name}.blob.core.windows.net/{blob_path}"
    df = spark.read.option("multiline", "true").json(file_path)
    
    print("JSON file successfully read from Azure Blob Storage")
    df.show()
    return df

def snowflake_connection_open():
    #Establish a Snowflake connection
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_OPTIONS["sfUser"],
        password=SNOWFLAKE_OPTIONS["sfPassword"],
        account=SNOWFLAKE_OPTIONS["sfURL"].replace("https://", "").split(".")[0],
        warehouse=SNOWFLAKE_OPTIONS["sfWarehouse"],
        database=SNOWFLAKE_OPTIONS["sfDatabase"],
        schema=SNOWFLAKE_OPTIONS["sfSchema"]
    )
    return conn, conn.cursor()

def snowflake_connection_close(cur, conn):
    #Closes the Snowflake connection
    cur.close()
    conn.commit()
    conn.close()

def create_snowflake_table(df):
    #Creates a Snowflake table dynamically based on JSON metadata
    meta_row = df.select("meta").first()
    if meta_row is None or meta_row["meta"] is None:
        raise ValueError("Meta data not found in JSON file")

    meta_json = meta_row["meta"].asDict(recursive=True)
    columns_metadata = meta_json.get("view", {}).get("columns", [])

    type_mapping = {
        "text": "STRING",
        "number": "STRING",  
        "timestamp": "STRING"
    }

    columns = []
    for col in columns_metadata:
        col_name = f'"{col["name"]}"'  
        col_type = type_mapping.get(col.get("dataTypeName", "text"), "STRING")
        columns.append(f"{col_name} {col_type}")

    table_name = "ElectricVehicleData"
    create_table_sql = f"CREATE OR REPLACE TABLE {table_name} (\n    " + ",\n    ".join(columns) + "\n);"

    print(create_table_sql)

    conn, cur = snowflake_connection_open()
    cur.execute(create_table_sql)
    print(f"Table {table_name} created successfully!")
    snowflake_connection_close(cur, conn)

def insert_to_snowflake(df):
    #Inserts data into Snowflake
    df_data = df.select("data").first()["data"]
    normalized_data = [tuple(map(str, row)) for row in df_data]

    meta_json = df.select("meta").first()["meta"].asDict(recursive=True)
    column_metadata = meta_json.get("view", {}).get("columns", [])
    column_names = [col["name"] for col in column_metadata]

    table_name = "ElectricVehicleData"
    columns_str = ", ".join(f'"{col}"' for col in column_names)
    placeholders = ", ".join(["%s"] * len(column_names))
    insert_sql = f'INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})'

    batch_size = 500
    conn, cur = snowflake_connection_open()
    
    for i in range(0, len(normalized_data), batch_size):
        batch = normalized_data[i: i + batch_size]
        cur.executemany(insert_sql, batch)
    
    print(f"Data successfully inserted into {table_name}!")
    snowflake_connection_close(cur, conn)



def main():
    global spark
    spark = create_spark_session("AzureBlobJson")
    df = read_json_from_azure()
    create_snowflake_table(df)
    insert_to_snowflake(df)


main()
