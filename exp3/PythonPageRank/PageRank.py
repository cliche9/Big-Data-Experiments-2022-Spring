from pyspark import SparkConf, SparkContext
import argparse
from operator import add

# input: sourceUrl \t targetUrls
# output: sourceUrl, targetUrls
def graph_builder(line):
    parts = line.split("\t")
    return parts[0], parts[1]

# input: sourceUrl, targetUrls
# output: sourceUrl, 1.0
def init_pagerank(parts):
    return parts[0], 1.0

# input: sourceUrl, PR, targetUrls
# output: targetUrl, PR / len(targetUrls)
def loop_pagerank(parts):
    target_urls = parts[1][1].split(",")
    target_pr = parts[1][0] / len(target_urls)
    
    for target_url in target_urls:
        yield(target_url, target_pr)

# .10f
def decimal_format(parts):
    return parts[0], float('%.10f' % parts[1])
    
    
def pagerank(sc, inpath, outpath, iterations):
    # GraphBuilder
    # @params: sourceUrl, targetUrls
    graph = sc.textFile(inpath).map(graph_builder).cache()
    
    # graph.foreach(print)
    
    # PageRankIterator
    # Initialization: sourceUrl, 1.0
    pr_value = graph.map(init_pagerank)
    
    # pr_value.foreach(print)
    
    for _ in range(iterations):
        
        # sourceUrl, PR, targetUrls
        contribs = pr_value.join(graph).flatMap(loop_pagerank)

        # contribs.foreach(print)
        
        # sourceUrl, PR
        pr_value = contribs.reduceByKey(add).mapValues(lambda pr : pr * 0.85 + 0.15)
        
        # pr_value.foreach(print)
        
    pr_value.sortBy(keyfunc = lambda x:x[1], ascending = False) \
            .map(decimal_format).coalesce(1).saveAsTextFile(outpath)
        
        
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='process some log messages, storing them and signaling a rest server'
    )
    parser.add_argument(
        '--appname',
        required=True,
        help='the name of the spark application (default: SparkPageRank)',
        default='SparkPageRank'
    )
    parser.add_argument(
        '--master',
        help='the master url for the spark cluster'
    )
    parser.add_argument(
        '--input',
        required=True,
        help='the input path of data for PageRank'
    )
    parser.add_argument(
        '--output',
        required=True,
        help='the output path of PageRank result'
    )
    args = parser.parse_args()
    # 初始化spark
    sconf = SparkConf().setAppName(args.appname)
    if args.master:
        sconf.setMaster(args.master)
    # 定义sparkContext
    sc = SparkContext(conf=sconf)
    
    pagerank(sc, args.input, args.output, 10)
