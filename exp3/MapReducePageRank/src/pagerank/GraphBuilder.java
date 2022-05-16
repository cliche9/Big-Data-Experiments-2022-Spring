package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * GraphBuilder: 建立网页链接关系图
 */
public class GraphBuilder {
    private static final double initPR = 1.0f;

    public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
        /**
         * Input
         *  key: 行偏移量
         *  value: 该行对应的文本信息
         * Output
         *  key: 源网站链接
         *  value: 目标网站链接列表
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] Urls = value.toString().split("\t");
            String sourceUrl = Urls[0];
            String targetUrls = Urls[1];
            // 此处以\t作为分割符
            context.write(new Text(sourceUrl), new Text(Double.toString(initPR) + '\t' + targetUrls));
        }
    }

    public static void main(String[] args) throws Exception {
    	Configuration conf = new Configuration();
    	Job job = Job.getInstance(conf, "GraphBuilder");
    	job.setJarByClass(GraphBuilder.class);
    	job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(GraphBuilderMapper.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}