from pyspark.sql import SparkSession
from delta.tables import DeltaTable
import os


#spark=SparkSession.builder.appName("Incremental").config("spark.jars", "/opt/spark/jars/mssql-jdbc-11.2.0.jre11.jar").getOrCreate()
spark = (
    SparkSession.builder
    .appName("Incremental")
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.2.0")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)
spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")
jdbc_url = "jdbc:sqlserver://batch_incremental-sqlserver-1:1433;databaseName=master;encrypt=true;trustServerCertificate=true"

properties={
    "user":"sa",
    "password":"Epi@10724",
    "driver":"com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

watermark_file="/tmp/spark/checkpoints/checkpoints.txt"

if os.path.exists(watermark_file):
    with open(watermark_file, "r") as file:
        last_watermark = file.read().strip()

    if not last_watermark or last_watermark.lower() == "none":
        last_watermark = "1900-01-01 00:00:00"
else:
    last_watermark = "1900-01-01 00:00:00"


print("last_watermark:",last_watermark)

query = f"""
(
SELECT *
FROM customers
WHERE updated_at > CAST('{last_watermark}' AS DATETIME)
) AS t
"""

df = spark.read.jdbc(url=jdbc_url, table=query, properties=properties)
output_path = "/tmp/bronze_delta"
if not DeltaTable.isDeltaTable(spark, output_path):
    df.write.format("delta").mode("overwrite").save(output_path)
else:
    df.write.format("delta").mode("append").save(output_path)
# df.write.format("delta").mode("append").save("/tmp/spark/output/spark_data/raw")

max_date = df.agg({"updated_at": "max"}).collect()[0][0]

if max_date is not None:
    formatted = max_date.strftime("%Y-%m-%d %H:%M:%S")
    with open("/tmp/spark/checkpoints/checkpoints.txt", "w") as f:
        f.write(formatted)
    print("Watermark updated to:", formatted)
else:
    print("No new records found. Watermark not updated.")

spark.stop()

