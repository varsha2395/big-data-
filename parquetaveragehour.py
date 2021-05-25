from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.types import DoubleType;
import time
start = time.time();
spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
df = spark.read.parquet('/home/output/stud05/parquetjan/part-00000-c97f875f-8bd0-4743-85ed-86556c043e8e-c000.snappy.parquet')
df1=df.withColumn('_c10',col("_c10").cast(DoubleType())) \
.withColumnRenamed("_c0","Year")\
.withColumnRenamed("_c1","Month")\
.withColumnRenamed("_c2","Day")\
.withColumnRenamed("_c3","Hour")\
.withColumnRenamed("_c8","site")
dftest=df1.filter((df1._c7=="o3") & (df1._c11.isNull()))
dftest2=dftest.withColumn("_c10", when(col("_c10").isNull(),-1).otherwise(col("_c10")))
#dftest2.show(50)
dftest3=dftest2.filter(dftest2._c10>0)
#dftest3.show(50)
dftest4=dftest3.groupBy("Year", "Month", "Day", "Hour","site") .avg("_c10").orderBy("year","Month","day","hour","site").withColumnRenamed("avg(_c10)","avg")
#dftest4.show(100)
dftest4.write.parquet("/home/output/stud05/parquefullfile1.parquet")
end=time.time();
print("Runtime of the program is",end-start);


