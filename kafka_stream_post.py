import os
import pyspark
from pyspark.sql import SparkSession

appName = 'Kafka Converter'
spark = SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()
server = '172.19.0.2:9092'
in_topic_name = 'post'
out_topic_name = 'processed_post'

df = spark \
  .readStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers", server) \
  .option("subscribe", in_topic_name) \
  .load()

print('########### Loading done ##########')
# df = df.selectExpr("CAST(id AS STRING) AS key", "to_json(struct(*)) AS value")
df.show()
print(type(df))
# ssc.start()
# ssc.awaitTermination()
df.writeStream.format("kafka").outputMode("append").option("kafka.bootstrap.servers", server).option("topic", out_topic_name).start().awaitTermination()