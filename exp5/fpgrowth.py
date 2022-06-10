from array import ArrayType
import re
from sqlalchemy.types import TEXT, VARCHAR
from pyspark import SparkConf, SparkContext
from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.types import ArrayType, IntegerType
import pyspark.sql.functions as f
import pandas as pd
import sqlite3
from tqdm import tqdm
import os

conf = SparkConf().setAppName('FPGrowth')
conf.set("spark.driver.memory", '2G')       # 一般这个 2G 就够了 不需要调整
conf.set("spark.executor.memory", '4G')     # 2个G的去调

spark = SparkSession(SparkContext(conf=conf))

all_ratings = spark.read.csv('exp5/ratings.csv', header=True, inferSchema=True, encoding='utf-8', sep=',')

# pandas读取sqlite, 存入spark
db_movies = sqlite3.connect('{}/instance/movie.sqlite'.format(os.getcwd()))
all_movies = spark.createDataFrame(
    pd.read_sql(
        'SELECT movieId FROM movies',
        db_movies,
    ).astype(int), ArrayType(IntegerType())
).selectExpr('value as movies').cache()
all_movies.show(5)

# 加标签, 判断用户是否喜欢该电影
all_ratings = all_ratings.withColumn('favorable', f.col('rating') > 3)
all_ratings.show(5)
favorable_ratings = all_ratings[all_ratings['favorable']]
# 用户分组, 确定每个用户喜欢电影的列表, 作为Transactions用于Aprior迭代
favorable_movies_by_user = favorable_ratings.groupby('userId').agg(f.collect_list('movieId').alias('movies')).cache()
favorable_movies_by_user.show(5)

fpGrowth = FPGrowth(itemsCol='movies', minSupport=0.2, minConfidence=0.6)
model = fpGrowth.fit(favorable_movies_by_user)
# 频繁项集
model.freqItemsets.show(10)
# 关联规则
model.associationRules.show(10)
# 根据关联规则的简单预测
model.transform(all_movies).show(10)
relatives = model.transform(all_movies).toPandas()
# 导出为csv
output = '{}/relatives.csv'.format(os.getcwd())
relatives.to_csv(output, sep=',', index=False, header=True)

def find_sims_by_genres(movieId, db_movies, k):
    genres = db_movies.execute(
        '''
        SELECT genres FROM movies WHERE movieId = (?)
        ''',
        (movieId, )
    ).fetchone()[0].split('|')

    relatives = set(
        db_movies.execute(
            '''
            SELECT movieId FROM movies WHERE genres LIKE (?)
            ''',
            ('%' + genres[0] + '%', )
        ).fetchall()
    )
    for keyword in genres[1:]:        
        movies = db_movies.execute(
            '''
            SELECT movieId FROM movies WHERE genres LIKE (?)
            ''',
            ('%' + keyword + '%', )
        ).fetchall()
        next = relatives & set(movies)
        if len(next) < 3:
            return relatives

    return list(relatives)[:k]

# 对空值使用其他方法补全, 此处简单使用同类型电影进行补全
appends = 3
for index, row in tqdm(relatives.iterrows()):
    # 遍历row, 对于relatives不足3个的进行补全
    num = len(row['prediction'])
    if (num < appends):
        movieId = row['movies'][0]
        movies = find_sims_by_genres(movieId, db_movies, appends)
        for i in range(appends - num):
            row['prediction'].append(movies[i])

print(relatives.head(10))

# 写回数据库
relatives.to_sql(
    name='movie_relations', 
    con=db_movies, 
    index=False, 
    if_exists='replace',
    dtypes={
        'movieId': VARCHAR(300),
        'relatives': TEXT,
    },
)