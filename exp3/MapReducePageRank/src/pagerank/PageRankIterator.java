package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PageRankIterator: 迭代计算PR值
 */
public class PageRankIterator {

    private static final double alpha = 0.85f;
    
    public static class PageRankMapper extends Mapper<Object, Text, Text, Text> {

        /**
         * Input
         *  key: 行偏移量
         *  value: 该行对应的文本信息
         *  Url + '\t' + PR + '\t' + TargetUrls
         * Output
         *  <Url, TargetUrls>, <TargetUrl, PR>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {
            // tmp[0]: PR值
            // tmp[1]: targetUrls, 以","作为分隔
            String[] tmp = value.toString().split("\t");
            double prValue = Double.valueOf(tmp[1]);
            String[] targetUrls = tmp[2].split(",");
            int n = targetUrls.length;
            for (int i = 0; i < n; i++) {
                context.write(new Text(targetUrls[i]), new Text(Double.toString(prValue / n)));
            }
            context.write(new Text(tmp[0]), new Text('#' + tmp[2]));
        }
    }

    public static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
        
        /**
         * Input
         *  <Url, PR>, <Url, TargetUrls>
         * 相同Url的会分到一块
         * Output
         *  <Url, PR + TargetUrls>
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double newPR = 0;
            String targetUrls = new String();
            for (Text value : values) {
                String str = value.toString();
                // 是key对应的出链
                if (str.startsWith("#"))
                    targetUrls = str.substring(1);
                // 其他节点对key的pr贡献值
                else
                    newPR += Double.valueOf(str);
            }
            newPR = newPR * alpha + (1 - alpha);    // pr = pr * alpha + (1 - alpha) / N
            context.write(key, new Text(Double.toString(newPR) + '\t' + targetUrls));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankIterator");
        job.setJarByClass(PageRankIterator.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(PageRankMapper.class);
        job.setReducerClass(PageRankReducer.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
