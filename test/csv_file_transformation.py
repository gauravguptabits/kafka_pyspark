# https://sparkbyexamples.com/pyspark-rdd/

import os
import pyspark
from pyspark.sql import SparkSession

appName = 'Expense Calculator'
spark = SparkSession.builder.appName(appName).config("spark.some.config.option", "some-value").getOrCreate()

rPath = os.path.join(os.getcwd(), 'data')
wPath = os.path.join(os.getcwd(), 'expense_by_product.csv')

df2 = spark.read.options(inferSchema=True, header=True).csv(rPath)
price_by_product_df = df2.groupby('title').sum()
price_by_product_df.show()
print(price_by_product_df.show())
price_by_product_df.write.csv(f'{wPath}')