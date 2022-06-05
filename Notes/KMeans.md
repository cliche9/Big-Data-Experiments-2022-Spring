[TOC]

# K-Means算法实现

## 实验要求

自行准备数据集，设计一种数据挖掘算法(聚类、分类、频繁项集挖掘或其他主题)对数据集进行信息提取，要求分别使用并行化和非并行化的方式实现该算法。实验环境可选择 Hadoop 或者 Spark，程序语言可选用 Java、Python、 Scala 等，在伪分布式环境下完成并行化算法的编写和测试，在单机环境下完成非并行化算法的编写和测试。

## 实验任务

对三通道的TIFF图进行颜色压缩，将原本8-bit的颜色表示压缩为4-bit的颜色表示，通过聚类对原图片的颜色（共256种）进行聚类，聚成16类；

原始图像数据

![bird_large_deserialized](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-bird_large_deserialized.tiff)

> 该图像来自Frank Wouters，仅用于学习用途；

## K-Means算法

### 核心思想

==K-Means聚类算法==本质是通过不断迭代，更新聚类中心点，将其他点分类到离其最近的聚类中心所属类即可，经证明，算法最终能够收敛；

具体迭代过程：

1. 计算每一个样本所属的聚类中心

   ![img](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-clip_image002.jpg)

2. 根据第一步的分类结果重新计算该类别的聚类中心

   ![image-20220520155015319](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-image-20220520155015319.png)

3. 不断地重复这个过程直到到达了指定的迭代次数或者聚类中心的变化不大

> 若存在数据线性不可分的情况，在计算距离的时候可以应用核函数以解决该问题；

常见的计算距离公式有欧几里得距离、曼哈顿距离、马哈拉诺比斯距离等；

==本实验中采用欧式距离作为距离的衡量标准==

## Python实现非并行化K-Means算法

![image-20220520155739351](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-image-20220520155739351.png)

* 蓝色框表示对数据点所属的类别进行计算，`label[i]`表示第`i`个数据点所属的类别`[0, 15]`；
* 红色框表示对聚类中心进行更新，`mu[j]`表示第`j`个聚类中心的坐标；

> K-Means聚类结束后，通过重新对图像对应像素颜色进行赋值，对图片进行序列化和反序列化即可展示对应聚类结束的图像

## MapReduce实现并行化K-Means算法

### KMeansIterator

> KMeans迭代器，根据之前的聚类点计算数据点的类别，然后计算并输出新的聚类中心坐标

#### KMeansIteratorMapper

* setup：读取全局共享的当前聚类中心坐标内容；

* Input

  * Key：文本偏移量
  * Value：一行文本内容，具体为输入点的坐标

* Output

  * Key：数据点类别
  * Value：数据点坐标

* 操作流程

  根据当前聚类点，计算数据点的类别，与当前点的坐标一块输出到reduce；

#### KMeansIteratorReducer

* Input

  * Key：数据点类别
  * Value：数据点坐标

* Output

  * Key：无
  * Value：新的聚类中心坐标

* 操作流程

  根据同一类的数据点，更新聚类中心坐标，输出到文件中；

### KMeansUpdater

> KMeans更新器，根据计算的聚类中心，将图片的像素颜色更新为对应分类颜色的值

#### KMeansUpdaterMapper

* setup：读取全局共享的当前聚类中心坐标内容；
* 操作流程
  * 读入数据点内容，对于每个数据点，计算其所属类别；
  * 将对应类别的坐标输出；

#### KMeansIteratorReducer

将Mapper输出的更新像素值按数据点原顺序输出到文件中；

### KMeansDriver

> 控制KMeansIterator迭代，使用KMeansUpdater输出最终结果

仅有一个main函数，提供：KMeans迭代-图像像素更新输出的逻辑；

## 实验结果

![bird_large_deserialized](https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-bird_large_deserialized.tiff)<img src="https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-py_out_img.tiff" alt="py_out_img" style="zoom:50%;" /><img src="https://fastly.jsdelivr.net/gh/cliche9/PicBeds/images/2022-05-20-mapred_out_img.tiff" alt="mapred_out_img" style="zoom:50%;" />

最上面是原图，下方左边是Python输出的压缩图像，右边是MapReduce输出的压缩图像；

