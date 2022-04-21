# -*- coding: utf-8 -*-
# @author: HUANG XM
# @create_time: 2022/04/20
'''
    pyspark实战入门第四章：数据处理
'''
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark import SparkContext
import pyspark.sql.functions as fn
from pyspark.sql.functions import rand

sc = SparkContext()

spark = SparkSession.builder.appName("DataFrame").getOrCreate()

# 去重
def set_data():
    df = spark.createDataFrame([
        (1, 144.5, 5.9, 33, 'M'),
        (2, 167.2, 5.4, 45, 'M'),
        (3, 124.1, 5.2, 23, 'F'),
        (4, 144.5, 5.9, 33, 'M'),
        (5, 133.2, 5.7, 54, 'F'),
        (3, 124.1, 5.2, 23, 'F'),
        (5, 129.2, 5.3, 42, 'M'),
    ], ['id', 'weight', 'height', 'age', 'gender'])
    print(f' df行数： {df.count()}')
    print(f' df去重行数: {df.distinct().count()}')
    df1 = df.dropDuplicates()
    df1.show()
    print('Count of ids: {0}'.format(df1.count()))
    print('Count of distinct ids: {0}'.format(df1.select([c for c in df.columns if c != 'id']).distinct().count()))
    df2 = df1.dropDuplicates(subset=[c for c in df1.columns if c != 'id'])
    df2.show()
    # agg方法
    df2.agg(fn.count('id').alias('count'), fn.countDistinct('id').alias('countDistinct')).show()

    df2.withColumn('new_id', fn.monotonically_increasing_id()).show()

# 缺失值
def miss_data():
    df_miss = spark.createDataFrame([
        (1, 143.5, 5.6, 28, 'M', 100000),
        (2, 167.2, 5.4, 45, 'M', None),
        (3, None, 5.2, None, None, None),
        (4, 144.5, 5.9, 33, 'M', None),
        (5, 133.2, 5.7, 54, 'F', None),
        (6, 124.1, 5.2, None, 'F', None),
        (7, 129.2, 5.3, 42, 'M', 76000),
    ], ['id', 'weight', 'height', 'age', 'gender', 'income'])
    df_miss.rdd.map(
        lambda row: (row['id'], sum([c == None for c in row]))
    ).collect()
    # 展示id为3的行，展示各列的确实比例
    df_miss.where('id == 3').show()
    df_miss.agg(*[
        (1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing')
        for c in df_miss.columns
    ]).show()
    # 去除空值多的income列
    df_miss_no_income = df_miss.select([c for c in df_miss.columns if c != 'income'])
    df_miss_no_income.show()
    # 以阈值，drop缺失量大于阈值的行
    df_miss_no_income.dropna(thresh=3).show()
    # fillna方法，填补缺失值，生成了一个key相同与列名的的dict 来fillna另一个dataframe
    means = df_miss_no_income.agg(
        *[fn.mean(c).alias(c) for c in df_miss_no_income.columns if c != 'gender']
    ).toPandas().to_dict('records')[0]
    print(type(means))

    means['gender'] = 'missing'
    means.show()
    df_miss_no_income.fillna(means).show()


# 异常值


if __name__ == '__main__':
    miss_data()
