# -*- coding: utf-8 -*-
# @author: HUANG XM
# @create_time: 2022/4/10
'''
    pyspark实战入门第三章：dataframe
'''
import re
import numpy as np
import pyspark
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.types import *


sc = SparkContext()

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

# Generate our own JSON data
#   This way we don't have to access the file system yet.
stringJSONRDD = sc.parallelize((""" 
  { "id": "123",
    "name": "Katie",
    "age": 19,
    "eyeColor": "brown"
  }""",
   """{
    "id": "234",
    "name": "Michael",
    "age": 22,
    "eyeColor": "green"
  }""",
  """{
    "id": "345",
    "name": "Simone",
    "age": 23,
    "eyeColor": "blue"
  }""")
)

def basic_dataframe():
    # Create DataFrame
    swimmersJSON = spark.read.json(stringJSONRDD)
    # 创建临时表
    swimmersJSON.createTempView("swimmersJSON")
    swimmersJSON.show(5)
    # sql查询dataframe
    sql_res = spark.sql('select * from swimmersJSON').collect()
    print(sql_res)
    # Print the schema
    schema = swimmersJSON.printSchema()
    print(schema)

# RDD到dataframe
def generate_dataframe():
    # 生成逗号分割的RDD
    stringCSVRDD = sc.parallelize([(123, 'Katie', 19, 'brown'), (234, 'Michael', 22, 'green'), (345, 'Simone', 23, 'blue')])
    schema = StructType(
        [
            StructField('id', LongType(), True),
            StructField('name', StringType(), True),
            StructField('age', LongType(), True),
            StructField('eyeColor', StringType(), True),
        ]
    )
    # 对RDD应使用schema模式并创建临时视图
    swimmers = spark.createDataFrame(stringCSVRDD, schema=schema)
    swimmers.createOrReplaceTempView('swimmers')
    swimmers.printSchema()
    print(swimmers.count())
    swimmers.select('id','age').filter('age = 22').show()
    swimmers.select(swimmers.id, swimmers.age).filter(swimmers.age == 22).show()
    swimmers.filter("eyeColor like 'b%'").show()
    spark.sql('select count(1) from swimmers').show()

    spark.sql('select * from swimmers where age == 22').show()

    spark.sql('select * from swimmers where eyeColor like "b%"').show()

    swimmers.show()


# generate_dataframe()

# csv数据读取和拼接的示例
def flight_data():
    # Set File Paths
    flightPerfFilePath = "/Users/mlamp/Desktop/github_test/learningPySpark-master/Chapter03/flight-data/departuredelays.csv"
    airportsFilePath = "/Users/mlamp/Desktop/github_test/learningPySpark-master/Chapter03/flight-data/airport-codes-na.txt"

    # Obtain Airports dataset
    airports = spark.read.csv(airportsFilePath, header='true', inferSchema='true', sep='\t')
    airports.createOrReplaceTempView("airports")

    # Obtain Departure Delays dataset
    flightPerf = spark.read.csv(flightPerfFilePath, header='true')
    flightPerf.createOrReplaceTempView("FlightPerformance")

    # Cache the Departure Delays dataset
    flightPerf.cache()

    spark.sql('select a.city, f.origin, sum(f.delay) as Delays from FlightPerformance f join airports a on a.IATA = f.origin'
              ' where a.State = "WA" group by a.city, f.origin order by sum(f.delay) desc').show()

    # spark.sql("select a.City, f.origin, sum(f.delay) as Delays from FlightPerformance f join airports a on a.IATA = f.origin
    # where a.State = 'WA' group by a.City, f.origin order by sum(f.delay) desc").show()


flight_data()