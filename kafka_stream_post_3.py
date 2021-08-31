# https://gist.github.com/cordon-thiago/2c12e05a3ef347c831bb054b4742392c#file-event-consumer-spark-ipynb

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType,StructField, StringType, IntegerType
from pyspark.sql import functions as sf

appName = 'Kafka Converter'
spark = SparkSession.builder.appName(appName).getOrCreate()
server = '172.18.0.2:9092'
in_topic_name = 'Posting'
out_topic_name = 'processedPosting'
checkpoint_dir = '/home/jovyan'

options = {
    'server': {
        'key': 'kafka.bootstrap.servers'
    }
}


schema = StructType([
    StructField("text",StringType(),True),
    StructField("user",StringType(),True)
])

negative_keywords = ['poor', 'bad', 'pathetic', 'crap', 'Shit']
positive_keywords = ['great', 'amazing', 'incredible', 'best']

df = spark.readStream.format("kafka").option("kafka.bootstrap.servers", server).option("subscribe", in_topic_name).load()
df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

df3 = df2.withColumn("value", from_json("value", schema))
df4 = df3.select(col("value.*"))

df5 = df4.where(df4['text'].rlike("|".join(["(" + pat + ")" for pat in negative_keywords])))
# df5 = df4.filter(df4.text.contains('Shit'))
df6 = df5.select(col("text"), col("user"))

df7 = df6.withColumn('value', sf.concat(sf.col('user'),sf.lit('::'), sf.col('text')))

query1 = df7.withColumnRenamed("user","key").writeStream.format("kafka").option("kafka.bootstrap.servers", server).option("topic", out_topic_name).option("checkpointLocation", checkpoint_dir).trigger(continuous="10 second").start()
query2 = df7.writeStream.format('console').start()

query1.awaitTermination()
query2.awaitTermination()
