import pandas as pd
from collections import defaultdict
from tqdm import tqdm

def data_info(data):
    print('Head:')
    print(data.head())

    data.info()

    print(data.shape)

    data.drop_duplicates(keep='first', inplace=True)
    print(data.shape)

    print('User Counts: {}'.format(data['userId'].value_counts()))
    print('Movie Counts: {}'.format(data['movieId'].value_counts()))
    print('Rating Counts: {}'.format(data['rating'].unique().tolist()))

all_ratings = pd.read_csv('../MLP-20M-Dataset/ratings.csv', header=0).sample(500000)
# data_info(all_ratings)

all_ratings['timestamp'] = pd.to_datetime(all_ratings['timestamp'], unit='s')

# 加标签, 判断用户是否喜欢该电影
all_ratings['favorable'] = all_ratings['rating'] > 3
favorable_ratings = all_ratings[all_ratings['favorable']]
# 用户分组, 确定每个用户喜欢电影的列表, 作为Transactions用于Aprior迭代
favorable_movies_by_user = dict(
    (k, frozenset(v.values))
    for k, v in favorable_ratings.groupby('userId')['movieId']
)
# 每部电影的喜欢人数 { key: movieId, value: 个数 }
num_favorable_by_movie = favorable_ratings[['movieId', 'favorable']].groupby('movieId').sum()
print("初始化了电影相关数据信息")
print("最受欢迎的10部电影")
print(num_favorable_by_movie.sort_values('favorable', ascending=False)[:10])

count = 0
for k, v in favorable_movies_by_user.items():
    count += len(v) > 5
print(f"喜欢电影 > 10的用户个数 = {count}")


def find_frequent_itemsets(favorable_movies_by_user, k_i_itemsets, min_support):
    counts = defaultdict(int)
    for user, movies in tqdm(favorable_movies_by_user.items()):
        for itemset in k_i_itemsets:
            if itemset.issubset(movies):
                for other_movies in movies - itemset:
                    current_superset = itemset | frozenset((other_movies, ))
                    counts[current_superset] += 1
    
    return dict(
        [
            (itemset, frequency) for itemset, frequency in counts.items()
            if frequency >= min_support
        ]
    )

# 频繁项集总列表, { key: 项集长度, value: 频繁项集列表 }
frequent_itemsets = {}
# 最小支持度
min_support = 5
# 初始化频繁1项集
frequent_itemsets[1] = dict(
    (frozenset((movie_id, )), row['favorable'])
    for movie_id, row in num_favorable_by_movie.iterrows()
        if row['favorable'] >= min_support
)

print("初始化了频繁1项集")

for k in range(2, 5):
    print(f"正在计算{k}项集...")
    cur_frequent_itemsets = find_frequent_itemsets(favorable_movies_by_user, frequent_itemsets[k - 1], min_support)
    if len(cur_frequent_itemsets) == 0:
        print(f"没有长度为{k}的频繁项集")
        break;
    else:
        print(f"长度为{k}的频繁项集有{len(cur_frequent_itemsets)}个")
        frequent_itemsets[k] = cur_frequent_itemsets
