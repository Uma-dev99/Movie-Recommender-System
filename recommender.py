from pyspark.sql import SparkSession
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.recommendation import ALS
import pandas as pd

spark = SparkSession.builder.appName('movielens').getOrCreate()

df = spark.read.csv(r'C:\Users\MSI\Desktop\ds project\ratings.csv',header=True,inferSchema=True)

# Descriptive Stats
df.head(3)
df.describe().show()
df.na.drop(how='any')
(train,test) = df.randomSplit([0.75,0.25])
