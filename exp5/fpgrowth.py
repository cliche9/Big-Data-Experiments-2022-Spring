from pickletools import pyset
from pyspark import SparkConf
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

spark = SparkSession\
    .builder\
    .appName("FPGrowthExample")\
    .getOrCreate()

all_ratings = spark.read.csv('exp5/ratings.csv', header=True, inferSchema=True, encoding='utf-8', sep=',')

# 加标签, 判断用户是否喜欢该电影
all_ratings = all_ratings.withColumn('favorable', f.col('rating') > 3)
all_ratings.show(5)
favorable_ratings = all_ratings[all_ratings['favorable']]
# 用户分组, 确定每个用户喜欢电影的列表, 作为Transactions用于Aprior迭代
favorable_movies_by_user = favorable_ratings.groupby('userId').agg(f.collect_list('movieId').alias('movies')).select('movies')
favorable_movies_by_user.show(5)

df_pred = spark.createDataFrame([
    ([1], [2], [3], [4], [5], ),
], ["movies"])

fpGrowth = FPGrowth(itemsCol='movies', minSupport=0.2, minConfidence=0.6)
model = fpGrowth.fit(favorable_movies_by_user)
# 频繁项集
model.freqItemsets.show(10)
# 关联规则
model.associationRules.show(10)
# 根据关联规则的简单预测
model.transform(df_pred).show(10)
