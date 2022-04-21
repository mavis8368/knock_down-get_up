# -*- coding: utf-8 -*-
# @author: HUANG XM
# @create_time: 2022/3/27
'''
    pyspark实战入门
'''
import re
import numpy as np
import pyspark
from pyspark import SparkContext

sc = SparkContext()

A = ['Aoejrn', 'BLEOU', 'Auebr', 'Atwtwhre']

data = sc.parallelize(A)


def extractInformation(row):
    selected_indices = [
        2, 4, 5, 6, 7, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18,
        19, 21, 22, 23, 24, 25, 27, 28, 29, 30, 32, 33, 34,
        36, 37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48,
        49, 50, 51, 52, 53, 54, 55, 56, 58, 60, 61, 62, 63,
        64, 65, 66, 67, 68, 69, 70, 71, 72, 73, 74, 75, 76,
        77, 78, 79, 81, 82, 83, 84, 85, 87, 89
    ]

    '''
        Input record schema
        schema: n-m (o) -- xxx
            n - position from
            m - position to
            o - number of characters
            xxx - description
        1. 1-19 (19) -- reserved positions
        2. 20 (1) -- resident status
        3. 21-60 (40) -- reserved positions
        4. 61-62 (2) -- education code (1989 revision)
        5. 63 (1) -- education code (2003 revision)
        6. 64 (1) -- education reporting flag
        7. 65-66 (2) -- month of death
        8. 67-68 (2) -- reserved positions
        9. 69 (1) -- sex
        10. 70 (1) -- age: 1-years, 2-months, 4-days, 5-hours, 6-minutes, 9-not stated
        11. 71-73 (3) -- number of units (years, months etc)
        12. 74 (1) -- age substitution flag (if the age reported in positions 70-74 is calculated using dates of birth and death)
        13. 75-76 (2) -- age recoded into 52 categories
        14. 77-78 (2) -- age recoded into 27 categories
        15. 79-80 (2) -- age recoded into 12 categories
        16. 81-82 (2) -- infant age recoded into 22 categories
        17. 83 (1) -- place of death
        18. 84 (1) -- marital status
        19. 85 (1) -- day of the week of death
        20. 86-101 (16) -- reserved positions
        21. 102-105 (4) -- current year
        22. 106 (1) -- injury at work
        23. 107 (1) -- manner of death
        24. 108 (1) -- manner of disposition
        25. 109 (1) -- autopsy
        26. 110-143 (34) -- reserved positions
        27. 144 (1) -- activity code
        28. 145 (1) -- place of injury
        29. 146-149 (4) -- ICD code
        30. 150-152 (3) -- 358 cause recode
        31. 153 (1) -- reserved position
        32. 154-156 (3) -- 113 cause recode
        33. 157-159 (3) -- 130 infant cause recode
        34. 160-161 (2) -- 39 cause recode
        35. 162 (1) -- reserved position
        36. 163-164 (2) -- number of entity-axis conditions
        37-56. 165-304 (140) -- list of up to 20 conditions
        57. 305-340 (36) -- reserved positions
        58. 341-342 (2) -- number of record axis conditions
        59. 343 (1) -- reserved position
        60-79. 344-443 (100) -- record axis conditions
        80. 444 (1) -- reserve position
        81. 445-446 (2) -- race
        82. 447 (1) -- bridged race flag
        83. 448 (1) -- race imputation flag
        84. 449 (1) -- race recode (3 categories)
        85. 450 (1) -- race recode (5 categories)
        86. 461-483 (33) -- reserved positions
        87. 484-486 (3) -- Hispanic origin
        88. 487 (1) -- reserved
        89. 488 (1) -- Hispanic origin/race recode
     '''

    record_split = re \
        .compile(
        r'([\s]{19})([0-9]{1})([\s]{40})([0-9\s]{2})([0-9\s]{1})([0-9]{1})([0-9]{2})' +
        r'([\s]{2})([FM]{1})([0-9]{1})([0-9]{3})([0-9\s]{1})([0-9]{2})([0-9]{2})' +
        r'([0-9]{2})([0-9\s]{2})([0-9]{1})([SMWDU]{1})([0-9]{1})([\s]{16})([0-9]{4})' +
        r'([YNU]{1})([0-9\s]{1})([BCOU]{1})([YNU]{1})([\s]{34})([0-9\s]{1})([0-9\s]{1})' +
        r'([A-Z0-9\s]{4})([0-9]{3})([\s]{1})([0-9\s]{3})([0-9\s]{3})([0-9\s]{2})([\s]{1})' +
        r'([0-9\s]{2})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})([A-Z0-9\s]{7})' +
        r'([A-Z0-9\s]{7})([\s]{36})([A-Z0-9\s]{2})([\s]{1})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})' +
        r'([A-Z0-9\s]{5})([A-Z0-9\s]{5})([A-Z0-9\s]{5})([\s]{1})([0-9\s]{2})([0-9\s]{1})' +
        r'([0-9\s]{1})([0-9\s]{1})([0-9\s]{1})([\s]{33})([0-9\s]{3})([0-9\s]{1})([0-9\s]{1})')
    try:
        rs = np.array(record_split.split(row))[selected_indices]
    except:
        rs = np.array(['-99'] * len(selected_indices))
    return rs


#     return record_split.split(row)
# 文件读取数据
data_from_file = sc.textFile('VS14MORT.txt', 4)
data_from_file_conv = data_from_file.map(extractInformation)

'''
    转换类方法
'''


# map方法 和 flatmap方法
def map_data():
    # data3 = data_from_file.map(lambda x: x[16])

    # 正则分割数据
    take1 = data_from_file_conv.map(lambda x: (x[16], int(x[16]) + 1))
    print("map:", take1.take(10))

    flatmap_data = data_from_file_conv.flatMap(lambda x: (x[16], int(x[16]) + 1))
    print("flatemap: ", (flatmap_data).take(10))


# map_data()


# filter方法
def filter_data():
    res = data.filter(lambda val: val.startswith('A')).collect()

    filtered = data_from_file_conv.filter(lambda x: x[16] == '2014' and x[21] == '0')
    print(filtered.count())
    print(filtered.take(1))


# filter_data()


# distinct方法
def distinct_data():
    distinct_gender = data_from_file_conv.map(lambda row: row[5]).distinct().collect()
    print(distinct_gender)


# distinct_data()

# sample
def sample_data():
    fraction = 0.1
    sampled = data_from_file_conv.sample(False, fraction, 99)
    print(data_from_file_conv.count(), sampled.count())


# sample_data()

# 同理有join ，intersection
def leftjoin():
    rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
    rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])

    rdd3 = rdd1.leftOuterJoin(rdd2)
    print(rdd3.take(5))

    rdd3 = rdd1.join(rdd2)
    print(rdd3.take(5))

    rdd3 = rdd1.intersection(rdd2)
    print(rdd3.take(5))


# leftjoin()

'''
    操作方法：对数据集的操作
'''

def f(x):
    print(x)

def action_func():
    rdd1 = sc.parallelize([('a', 1), ('b', 4), ('c', 10)])
    rdd2 = sc.parallelize([('a', 4), ('a', 1), ('b', '6'), ('d', 15)])
    # take(n)，取数据集前n个数据
    # data_from_file_conv.take(2)
    # 随机取
    # data_from_file_conv.takeSample(False, 2, 99)
    # 执行
    # data_from_file_conv.collect()
    # reduce，输出总数
    res = rdd1.map(lambda row: row[1]).reduce(lambda x, y: x + y)
    print(res)
    # 分区会产生错误
    data_reduce = sc.parallelize([1, 2, .5, .1, 5, .2], 1)
    # data_reduce.reduce(lambda x, y: x / y)
    # 在键的基础上reduce，reducebykey
    data_key = sc.parallelize([('a', 4),('b', 3),('c', 2),('a', 8),('d', 2),('b', 1),('d', 3)],4)
    # data_key.reduceByKey(lambda x, y: x + y).collect()
    # count计数
    print(data_reduce.count())
    print(data_key.countByKey().items())

    # 数据存储
    # data_key.saveAsTextFile('data_key.txt')

    data_key.foreach(f)


action_func()

# 读取存储的数据
def parseInput(row):
    import re
    pattern = re.compile(r'\(\'([a-z])\', ([0-9])\)')
    row_split = pattern.split(row)
    return (row_split[1], int(row_split[2]))

data_key_reread = sc \
    .textFile('data_key.txt') \
    .map(parseInput)
# print(data_key_reread.collect())

