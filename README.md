# HadoopLab
Bigdata Practice in CS of NJU
##代码目录结构说明
提供优化后源码文件，下面说明各个文件：
  * **HadoopTool.java：**           包含了一些小tool
  * **JDriver.java：**              整个pipeline过程的driver实现
  * **LPA.java：**                  LPA算法的map-reduce实现
  * **PageRankIter.java：**         pagerank算法的map-reduce实现
  * **PageRankLPAIter.java：**      pagerank和lpa整合的map-reduce实现
  * **PageRankSort.java：**         按照pagerank值进行排序的map-reduce实现
  * **PreProcess.java：**           对小说进行预处理并生成人名文件的map-reduce实现
  * **TaskTwo.java：**              生成邻接图和进行权值归一化的map-reduce实现
