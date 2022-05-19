[TOC]

# PageRank算法实现

## 实验要求

1. 分别使用 MapReduce 和 Spark 实现 PageRank 算法，Spark程序可采用 Java、Python、Scala 等语言进行编程，编程工具、语言自由选定

   * Dataset：数据集中每一行内容的格式:网页+\t+该网页链接到的网页的集合(相互之间用 英文逗号分开)

   * 输出：能够利用PageRank算法的思想计算出每个网页的PR值(迭代10次即可)

     ##### ![image-20220423165926995](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-04-23%2016-59-27%20image-20220423165926995.png)

2. 在伪分布式环境下完成程序的编写和测试
3. 在集群上提交作业并执行

## PageRank算法思路

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

### GraphBuilder

> 建立网页链接关系图，初始化各网页的PR值为1.0

#### GraphBuilderMapper

* Input

  * Key：DataSet文本文件的行偏移量
  * Value：该行对应的源网页及其出链信息

* Output

  * Key：源网站链接
  * Value：**初始PR值 + "\t" + 目标网站链接列表**

* 操作流程

  读取每一行的源网页，目标网页的链接关系，对每个网页赋予初始PR值，输出格式为**`<sourceUrl, PR + "\t" + targetUrls>`**，其中`targetUrls`是目标网页的列表；

#### GraphBuilderReducer

无明显操作，直接输出`Mapper`结果即可；

### PageIterator

> PageRank迭代器，读取相应网页链接关系，更新各个网页的PR值

#### PageIteratorMapper

* Input
  * Key：迭代过程临时文本文件的行偏移量
  * Value：该行对应的源网页、PR值、出链信息
  
* Output

  ==Output分两种，分别输出网页PR值和对应的出链信息==

  `<Url, targetUrls>`和`<targetUrl, Url对TargetUrl的PR贡献值>`

* 操作流程

  读取临时文件中的源网页PR值及其出链信息，输出源网页对可达网页的PR贡献值，同时输出源网页、目标网页的链接关系以供下次迭代使用；

#### PageIteratorReducer

* Input

  有两种，分别为`<Url, targetUrls>`和`<Url, 其他网页对当前Url的PR贡献值>`

* Output

  以**`<Url, PR + "\t" + targetUrls>`**输出迭代结果，迭代结果为更新后的当前网页PR值及其对应的出链信息；

* 操作流程

  `Map`过程以`Url`为Key进行分组和排序，`Reduce`过程整合同一网页的PR值和出链信息；

  * 对于PR值，使用公式
    $$
    PR(X) = \alpha \sum_{Y_i \in S(X)}PR(Y_i) + (1-\alpha)
    $$
    简单计算每个节点$X$的新PR值；

  * 对于出链信息，维护输出格式为**`<Url, PR + "\t" + targetUrls>`**，将含有新的PR值的内容输出到临时文件中以供下一次迭代使用；

#### 注意事项

* PageRankIterator作为迭代器，需要维护输入输出格式的一致性，这是实现迭代运算的基础；
* 新的PR值采取简易计算，若引入出边个数等变量则整个计算过程相对复杂；

### PageViewer

> PageRank输出器，负责将迭代最终结果有序输出

#### PageViewerMapper

* Input

  迭代最终输出结果，格式为**`<Url, PR + "\t" + targetUrls>`**；

* Output

  提取对应**`<Url, PR值>`**输出即可；

#### Shuffle && Sort流程

**实验要求按PR值降序输出对应的网页；**

此处利用MapReduce框架的Shuffle && Sort逻辑进行实现，Shuffle && Sort默认为按Key值升序输出，为了将其更改为降序，我们修改Key值对应数据结构的比较器；

```java
public static class DescDoubleComparator extends DoubleWritable.Comparator {
  // 修改连续compare函数, 提供多级比较支持
  public double compare(WritableComparator cmp, WritableComparable<DoubleWritable> value) {
    return -super.compare(cmp, value);
  }
  // 修改序列化的compare函数
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    return -super.compare(b1, s1, l1, b2, s2, l2);
  }
}
```

并通过设置比较器`job.setSortComparatorClass(DescDoubleComparator.class);`来实现降序排列；

#### PageViewerReducer

* Input

  降序排列的**`<Url, PR值>`**，对其进行格式封装输出；

* Output

  PR值保留10位小数，Key值输出格式为`(Url, PR值)`，Value设为null；

### PageDriver

> 控制GraphBuilder、PageRankIterator迭代、PageViewer输出最终结果

仅有一个main函数，提供：网页链接关系图初始化-PR值迭代-最终结果输出的逻辑；

## Python实现PageRank算法

### 初始化Spark，定义Spark的上下文

### 启动PageRank迭代函数进行PR值的求解计算

* 初始化网页链接关系图`graph`，生成对应RDD，表项为**`<Url, targetUrls>`**,将该链接关系图进行`cache()`持久化以提高访问性能；

* 设置迭代所用RDD：`pr_value`，`pr_value`结构为**`<Url, PR值>`**集合的表；

* 迭代，根据公式
  $$
  PR(X) = \alpha \sum_{Y_i \in S(X)}PR(Y_i) + (1-\alpha)
  $$
  更新每个网页对应的PR值，具体实现过程如下：

  * `graph`和`pr_value`通过连接操作得到结构为**`<Url, (PR值, targetUrls)>`**的RDD；

  * 通过RDD的`flatMap()`调用自定义的`loop_pagerank`函数计算网页pr值的离散分布RDD，`contribs`；

    > loop_pagerank：
    >
    > ```python
    > def loop_pagerank(parts):
    >     target_urls = parts[1][1].split(",")
    >     target_pr = parts[1][0] / len(target_urls)
    >     
    >     for target_url in target_urls:
    >         yield(target_url, target_pr)
    > ```

  * `contribs`经

    ```python
    contribs.reduceByKey(add).mapValues(lambda pr : pr * 0.85 + 0.15)
    ```

    得到新的`pr_value`；

* 迭代完毕，降序输出最终的**`<Url, PR值>`**

  ```python
  pr_value.sortBy(keyfunc = lambda x:x[1], ascending = False).map(decimal_format).coalesce(1).saveAsTextFile(outpath)
  ```

### 注意事项

* 利用PySpark实现的PageRank输出结果中，Url字段输出格式为\`Url\`，与标准输出略有不同；
* `loop_pagerank`中通过`yield`和`flatMap`实现了网页pr值的离散化，完成1对多映射；

## 实验总结

### MapReduce

PageRank实现方案原始，利用了多个MapReduce过程才完成了PageRank算法；

### Spark

通过简短的函数调用和RDD的操作即可完成对应PageRank算法，但需要熟悉Spark的若干Transformation和Action操作，将其简单总结如下；

* Transformation：记录变换，但并不真实计算
  * `filter(func)`：筛选出满足函数func的元素，并返回一个新的RDD
  * `map(func)`：将每个元素传递到函数func中，并将结果返回为一个新的RDD
  * `flatMap(func)`：与`map()`相似，但每个输入元素都可以映射到0或多个输出结果
  * `groupByKey()`：应用于(K,V)键值对的RDD时，返回一个新的(K, Iterable)形式的RDD
  * `reduceByKey(func)`：应用于(K,V)键值对的RDD时，返回一个新的(K, V)形式的RDD集，其中的每个值是将每个key传递到函数func中进行聚合
* Action：触发真实计算，同时依次执行之前的Transformation操作
  * `count()` 返回RDD中的元素个数
  * `collect()` 以数组的形式返回RDD中的所有元素
  * `first()` 返回RDD中的第一个元素
  * `take(n)` 以数组的形式返回RDD中的前n个元素
  * `reduce(func)` 通过函数func（输入两个参数并返回一个值）聚合RDD中的元素
  * `foreach(func)` 将RDD中的每个元素传递到函数func中运行
