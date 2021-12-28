from pyspark import SparkConf
from pyspark.sql import SparkSession

conf = SparkConf()
conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.2.0')

conf.set('spark.hadoop.fs.s3a.access.key', AccessKey)
conf.set('spark.hadoop.fs.s3a.secret.key', SecretKey)

spark = SparkSession.builder.config(conf=conf).getOrCreate()
myDF = spark.read\
    .format("csv")\
    .option("header", "true")\
    .option("inferSchema","true")\
    .csv("s3a://demo-py-spark/")
print(type(myDF))
myDF.printSchema()
myDF.show(truncate=False)
