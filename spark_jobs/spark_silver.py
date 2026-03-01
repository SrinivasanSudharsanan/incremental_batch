from pyspark.sql import SparkSession
from delta.tables import DeltaTable
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number,col,current_timestamp,lit

spark=SparkSession.builder.appName('silver_job').getOrCreate()

spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

bronze_df=spark.read.format("delta").load("/tmp/spark/output/data/raw")

window = Window.partitionBy("id").orderBy(col("updated_at").desc())

bronze_df = bronze_df.withColumn(
    "rn",
    row_number().over(window)
).filter("rn = 1").drop("rn")
silver_path="/tmp/spark/data/silver"

batch_id = "2026_02_28_batch1"
bronze_df = bronze_df \
    .withColumn("ingestion_time", current_timestamp()) \
    .withColumn("batch_id", lit(batch_id))

if DeltaTable.isDeltaTable(spark, silver_path):
    silver_table=DeltaTable.forPath(spark,silver_path)
    silver_table.alias("t").merge(
    bronze_df.alias("s"),
    "t.id = s.id"
    ).whenMatchedUpdate(
        condition="""
            t.name <> s.name OR
            t.email <> s.email OR
            t.updated_at <> s.updated_at""",
        set={
            "name": "s.name",
            "email": "s.email",
            "updated_at": "s.updated_at",
            "ingestion_time": "s.ingestion_time",
            "batch_id": "s.batch_id"
            }
        ).whenNotMatchedInsert(
            values={
            "id": "s.id",
            "name": "s.name",
            "email": "s.email",
            "updated_at": "s.updated_at",
            "ingestion_time": "s.ingestion_time",
            "batch_id": "s.batch_id"
            }
            ).execute()

else:
    bronze_df.write.format("delta").save("/tmp/spark/data/silver")
