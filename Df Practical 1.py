from pyspark import SparkConf
from pyspark.sql import SparkSession

if __name__ == '__main__':
    print("Start")


my_conf = SparkConf()
my_conf.set("spark.app.name", "my first application")
my_conf.set("spark.master", "local[*]")

spark = SparkSession.builder.config(conf= my_conf).getOrCreate()

# External Schema
orderDDL = "orderid Integer ,orderdate Timestamp,customerid Integer,status String"

orderDf = spark.read.format("csv")\
            .option("header", True)\
            .schema(orderDDL)\
            .option("path", "file:///C://Users//krnsa//Downloads//orders-201019-002101.csv")\
            .load()

GroupDf = orderDf.repartition(4)\
.where("customerid > 1000")\
.select("orderid" , "customerid")\
.groupBy("customerid")\
.count()

GroupDf.show()
orderDf.printSchema()

spark.stop()
