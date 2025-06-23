from pyspark.sql import SparkSession
from pyspark.sql.functions import col

spark = SparkSession.builder.appName("Silver to Gold").getOrCreate()

df = spark.read.parquet("/data/silver/cleaned_data.parquet")
result = df.groupBy("category").count()
result.write.mode("overwrite").csv("/data/gold/final_report.csv", header=True)

spark.stop()
