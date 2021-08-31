# https://gist.github.com/cordon-thiago/2c12e05a3ef347c831bb054b4742392c#file-event-consumer-spark-ipynb

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
import os
from pyspark.sql.functions import from_json, col


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

# df = spark.read.format("kafka").option("kafka.bootstrap.servers", server).option("subscribe", in_topic_name).load()
# casted_df = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
# print('#########################', type(casted_df))
# dfStream = df5.writeStream.format("kafka").option("kafka.bootstrap.servers", server).option("topic", out_topic_name).option("checkpointLocation", checkpoint_dir).trigger(continuous="10 second")
# query = dfStream.start()
# query.awaitTermination()

wPath = os.path.join(os.getcwd(), 'kafka_data.csv')
# casted_df.write.mode('overwrite').csv(f'{wPath}')

df2 = spark.read.options(inferSchema=True, header=False).csv(wPath)
df2.printSchema()
df2.show(10)

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

schema = StructType([
    StructField("text",StringType(),True),
    StructField("user",StringType(),True)
])

# df2 = casted_df
df3 = df2.withColumn("_c1", from_json("_c1", schema))
df4 = df3.select(col("_c1.*"))
df4.show()

df5 = df4.filter(df4.text.contains("Shit"))
df6 = df5.select(col("text"), col("user"))
df6.show()

