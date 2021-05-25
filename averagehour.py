from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when
from pyspark.sql.types import DoubleType;
import time;
start=time.time();
spark = SparkSession.builder.appName("how to read csv file").getOrCreate()
df = spark.read.csv('/home2/input/2013_data_full.csv')
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
dftest4=dftest3.groupBy("Year", "Month", "Day", "Hour","site") .avg("_c10").orderBy("year","Month","day","hour","site")
#dftest4.show(100)
#print(dftest4.count())
#print(dftest4.select("site").distinct().count())
dftest4.write.option("header","true").csv("/home/output/stud05/fulloutput4")
end=time.time();
print("Runtime of the program is",end-start);
