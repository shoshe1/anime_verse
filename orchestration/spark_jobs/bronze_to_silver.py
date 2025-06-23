from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Bronze to Silver").getOrCreate()

df = spark.read.csv("/data/bronze/input_data.csv", header=True, inferSchema=True)
df_clean = df.dropna()
df_clean.write.mode("overwrite").parquet("/data/silver/cleaned_data.parquet")

spark.stop()
