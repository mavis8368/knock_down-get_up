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
import pyspark.sql.types as typ

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
    print('Count of distinct ids: {0}'.format(df1.select([c for c in df.columns if c != 'id']).distinct().show()))
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
    # 空值映射到df_miss
    miss_content = df_miss.rdd.map(
        lambda row: (row['id'], sum([c == None for c in row]))
    ).collect()

    print(miss_content)

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

    means['gender'] = 'missing'
    df_miss_no_income.fillna(means).show()


# 异常值
def outliner():
    # 先创建个dataframe
    df_outliers = spark.createDataFrame([
        (1, 143.5, 5.3, 28),
        (2, 154.2, 5.5, 45),
        (3, 342.3, 5.1, 99),
        (4, 144.5, 5.5, 33),
        (5, 133.2, 5.4, 54),
        (6, 124.1, 5.1, 21),
        (7, 129.2, 5.3, 42),
    ], ['id', 'weight', 'height', 'age'])

    cols = ['weight', 'height', 'age']
    bounds = {}

    for col in cols:
        # 参数分别，列名，分位数或分位数列表，可接受的错误程度（指结果非精确值
        quantiles = df_outliers.approxQuantile(col, [0.25, 0.75], 0.05)
        IQR = quantiles[1] - quantiles[0]
        bounds[col] = [quantiles[0] - 1.5 * IQR, quantiles[1] + 1.5 * IQR]
    print(bounds)
    outliers = df_outliers.select(
        *['id'] + [((df_outliers[c] < bounds[c][0]) | (df_outliers[c] > bounds[c][1])).alias(c + '_o') for c in cols])
    outliers.show()

    # 以id列join两个dataframe
    df_outliers = df_outliers.join(outliers, on='id')
    df_outliers.filter('weight_o').select('id', 'weight').show()
    df_outliers.filter('age_o').select('id', 'age').show()


# 数据理解，ccFraud数据集分析
def data_analysis():
    fraud = sc.textFile('/Users/mlamp/knock_down-get_up/ccFraud.csv')
    header = fraud.first()
    fraud = fraud.filter(lambda row: row != header).map(lambda row: [int(x) for x in row.split(",")])

    # 构建schema
    field =[
         *[typ.StructField(h[1:-1], typ.IntegerType(), True) for h in header.split(',')]
    ]

    schema = typ.StructType(field)

    # 创建dataframe
    fraud_df = spark.createDataFrame(fraud, schema=schema)
    fraud_df.createOrReplaceTempView('fraud')
    fraud_df.printSchema()

    # 分类列的统计
    # fraud_df.groupby('gender').count().show()

    # 统计性描述
    # numerical = ['balance', 'numTrans', 'numIntlTrans']
    # desc = fraud_df.describe(numerical)
    # desc.show()

    # fraud_df.agg({'balance': 'skewness'}).show()
    # fraud_df.agg({'balance': 'mean'}).show()

    fraud_df.corr()

    # 可视化
    hists = fraud_df.select('balance').rdd.flatMap(lambda row: row).histogram(20)

if __name__ == '__main__':
    # set_data()
    num = 0
    for i in range(3, 100):
        for j in range(3, 100):
            if 2/i + 2/j > 1:
                print(i,j)
                print(2/i, 2/j)
                num += 1
            else:
                break
    print(num)