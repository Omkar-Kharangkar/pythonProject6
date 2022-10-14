from pyspark.sql import *
from pyspark.sql import functions as F
import sys
import os

spark = SparkSession.builder.master("local").appName("testing").getOrCreate()
sc=spark.sparkContext
data=sys.argv[1]
#data="D:\\bigdata\\datasets\\bank-full.csv"
df = spark.read.format('csv').option("header","true").option("inferSchema","true").option("sep",";").load(data).cache()
df.createOrReplaceTempView("sales")
res=spark.sql("select * from sales where balance>50000 and age<90")
res.show()
res.printSchema()
#spark-submit --master local --deploy-mode client csvdata.py