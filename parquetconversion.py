from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
df = spark.read.csv('/home2/input/2013_data_full.csv')
df.repartition(1).write.mode('overwrite').parquet('/home/output/stud05/parquetjan')
