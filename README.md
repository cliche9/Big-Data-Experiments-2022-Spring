# 山东大学深入理解大数据实验 2022 春季学期
## 实验零 Hadoop入门
* 安装Hadoop单机和伪分布式系统
* 运行WordCount文件，完成词频统计任务
## 实验一 大数据系统基本实验
* 熟悉常用的 Linux 操作和 Hadoop 操作

* 熟悉常用的 HDFS 操作

* 熟悉常用的 HBase 操作

* NoSQL 和关系数据库的比较

  - MySQL

  - HBse

  - Redis

  - MongoDB

* MapReduce 初级编程

* 编程实现文件的合并和去重

* 编程实现对输入文件的排序

* 对指定的表格进行信息挖掘（祖辈关系）
## 实验二 文档倒排索引算法实现

* MapReduce编程实现倒排索引（Inverted Index）结构

  * Dataset：停词表，文档数据集

  * 输出：要求程序能够实现对 stop-words(如 a,an,the,in,of 等词)的去除，能够统计单词在 每篇文档中出现的频率

    ![image-20220423165848649](https://cdn.jsdelivr.net/gh/cliche9/PicBeds/images/2022-04-23%2016-58-50%20image-20220423165848649.png)

* 本地伪分布式环境下完成程序编写调试

* 提交到集群中执行作业

## 实验三 PageRank 算法实现

* 分别使用 MapReduce 和 Spark 实现 PageRank 算法，Spark程序可采用 Java、Python、Scala 等语言进行编程，编程工具、语言自由选定

  * Dataset：数据集中每一行内容的格式:网页+\t+该网页链接到的网页的集合(相互之间用 英文逗号分开)

  * 输出：能够利用PageRank算法的思想计算出每个网页的PR值(迭代10次即可)

    ![image-20220423165926995](https://cdn.jsdelivr.net/gh/cliche9/PicBeds/images/2022-04-23%2016-59-27%20image-20220423165926995.png)

* 在伪分布式环境下完成程序的编写和测试

* 在集群上提交作业并执行