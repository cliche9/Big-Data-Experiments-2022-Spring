# 基于Apriori算法的电影推荐系统

## 实验要求

1. 使用 Spark 实现 Apriori 算法，用于电影推荐系统；
   * Dataset：MovieLens-20M数据集，包含电影信息及用户评分等
   * 输出：对于某个电影，推荐至少3个用户可能喜欢的电影
2. 使用Python、Flask建立电影推荐系统，提供简单易用的使用界面；

## Apriori算法思路

### 核心思想

网站之间的链接关系通过有向图表示，$A \rightarrow B$表示网站A有到达网站B的链接；
$$
PR(A) = \sum_K \frac{PR(K)}{n_K}
$$
此处满足$K \rightarrow A$，$n_K$是节点$K$的出边个数；

==存在两种特殊情况：==

* RankLeak：考虑没有出边的网站，设网站A没有出边，则$PR(A)$被均分到图中其他所有点；

* RankSink：考虑没有入边的网站，采用随机浏览模型处理；

  设网站$A\rightarrow B$而A与C、D不邻接；

  则网站A有$\alpha PR(A)$的概率访问网站B，有$\frac{1-\alpha}{2}$的概率访问网站C和网站D；

### 一般化的PageRank表达式

$$
PR(X) = \alpha \sum_{Y_i \in S(X)}\frac{PR(Y_i)}{n_i} + \frac{1-\alpha}{N}
$$

$S(X)$为直接可达$X$的节点集合

$\alpha$为阻尼系数，一般为0.85

$N$为网页总数

$n_i$为网页$Y_i$邻接的网页数

### 实现思路

1. 建立网页链接有向关系图；
2. 迭代，根据每个网页节点的入链重复计算每个网页节点对应的PR值；
3. PR值趋于稳定/达到迭代次数，结束迭代，输出对应的网页PR值列表；

## MapReduce实现PageRank算法