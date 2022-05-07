# PageRank算法实现

* 分别使用 MapReduce 和 Spark 实现 PageRank 算法，Spark程序可采用 Java、Python、Scala 等语言进行编程，编程工具、语言自由选定

  * Dataset：数据集中每一行内容的格式:网页+\t+该网页链接到的网页的集合(相互之间用 英文逗号分开)

  * 输出：能够利用PageRank算法的思想计算出每个网页的PR值(迭代10次即可)

    ![image-20220423165926995](https://cdn.jsdelivr.net/gh/cliche9/PicBeds/images/2022-04-23%2016-59-27%20image-20220423165926995.png)

* 在伪分布式环境下完成程序的编写和测试

* 在集群上提交作业并执行

## PageRank算法思路

核心思想

* 网站之间的链接关系通过有向图表示，$A \rightarrow B$表示网站A有到达网站B的链接；
  $$
  PR(A) = \sum_K^N \frac{PR(K)}{n_K}
  $$

* RankLeak：考虑没有出边的网站，设网站A没有出边，则$PR(A)$被均分到图中其他所有点；

* RankSink：考虑没有入边的网站，采用随机浏览模型处理；

  设网站$A\rightarrow B$而A与C、D不邻接；

  则网站A有$\alpha PR(A)$的概率访问网站B，有$\frac{1-\alpha}{2}$的概率访问网站C和网站D；

* 一般化的PageRank表达式
  $$
  PR(X) = \alpha \sum_{Y_i \in S(X)}\frac{PR(Y_i)}{n_i} + \frac{1-\alpha}{N}
  $$
  $\alpha$为阻尼系数，一般为0.85

  N为网页总数

  $n_i$为网页X邻接的网页数

* 

实现思路

* 迭代法

  

## MapReduce实现PageRank算法



## Python实现PageRank算法

