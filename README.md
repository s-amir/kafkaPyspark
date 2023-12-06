# kafkaPyspark

**Versions Dependency:**
- pyspark: 3.5.0
- kafka-connector: org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0
- Java: 8

- ## write in batch mode:  
create dataframe with random name and age 
create column named value(required) to insert value:   
`df=spark.createDataFrame(data,schema).select(to_json(struct(col('name'),col('age'))).alias('value'))`  
write dataframe in specific topic:  
`df.write.format('kafka').option("kafka.bootstrap.servers", 'localhost:9092').option("topic", "spark1").save()`

- ## read in batch mode:    
`df=spark.read.format('kafka').option("kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load()
df.selectExpr("CAST(value AS STRING)").show(truncate=False)`

- ## read stream:  
`df = spark.readStream.format('kafka').option("checkpointLocation", "/tmp/kafkaPyspark").option("forceDeleteTempCheckpointLocation", "true")\
    .option("startingOffsets", "earliest")\
    .option(
    "kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load() \
    .writeStream.format('console').option("forceDeleteTempCheckpointLocation", "true").start().awaitTermination()`

   > note: determine startingOffset  
    for reading from topic subscribe to topic like: _.option("subscribe", "spark1")_

- ## write stream --> from streamed data(combined read and write stream)  
` df = spark.readStream.format('kafka').option("forceDeleteTempCheckpointLocation", "true")\
    .option("startingOffsets", "earliest")\
    .option(
    "kafka.bootstrap.servers", 'localhost:9092').option("subscribe", "spark1").load() \
    .writeStream.format('kafka') .option(
    "kafka.bootstrap.servers", 'localhost:9092').option("topic", "spark2").option("checkpointLocation", "/tmp/kafkaPyspark1").start().awaitTermination()`

    
