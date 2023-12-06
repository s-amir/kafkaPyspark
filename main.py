from random import randint

from faker import Faker
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json, struct

spark = SparkSession.builder.appName("kafka").config("spark.jars.packages",
                                                     "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0").getOrCreate()
#
# fake=Faker()
# data=[(fake.name(),randint(18, 70)) for _ in range(200)]
# schema = ["name", "age"]
#
# df=spark.createDataFrame(data,schema).select(to_json(struct(col('name'),col('age'))).alias('value'))
#
# print(df.schema)
#
# df.write.format('kafka').option("kafka.bootstrap.servers", 'localhost:9092').option("topic", "spark1") \
#     .save()


# _________________________________________________________________________________________________________________
# df=spark.read.format('kafka').option("kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load()
# df.selectExpr("CAST(value AS STRING)").show(truncate=False)

# ___________________________________________________________________________________________________________________
# df = spark.readStream.format('kafka').option("checkpointLocation", "/tmp/kafkaPyspark").option("forceDeleteTempCheckpointLocation", "true")\
#     .option("startingOffsets", "earliest")\
#     .option(
#     "kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load() \
#     .writeStream.format('console').option("forceDeleteTempCheckpointLocation", "true").start().awaitTermination()
#___________________________________________________________________________________________________________________________________

# df = spark.readStream.format('kafka').option("forceDeleteTempCheckpointLocation", "true")\
#     .option("startingOffsets", "earliest")\
#     .option(
#     "kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load() \
#     .writeStream.format('kafka') .option(
#     "kafka.bootstrap.servers", 'localhost:9092').option("topic", "spark2").option("checkpointLocation", "/tmp/kafkaPyspark1").start().awaitTermination()