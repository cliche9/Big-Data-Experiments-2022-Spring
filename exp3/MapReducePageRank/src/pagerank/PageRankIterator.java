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

    private static final float alpha = 0.85f;
    
    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {

        /**
         * Input
         *  <Url, PR + '\t' + TargetUrls>
         * Output
         *  <Url, TargetUrls>, <TargetUrl, PR>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {
            // tmp[0]: PR值
            // tmp[1]: targetUrls, 以","作为分隔
            String[] tmp = value.toString().split("\t");
            float prValue = Float.valueOf(tmp[0]);
            String[] targetUrls = tmp[1].split(",");
            int n = targetUrls.length;
            for (int i = 0; i < n; i++) {
                context.write(new Text(targetUrls[i]), new Text(Float.toString(prValue / n)));
            }
            context.write(key, new Text('#' + tmp[1]));
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
            float newPR = 0;
            String targetUrls = new String();
            for (Text value : values) {
                String str = value.toString();
                // 是key对应的出链
                if (str.startsWith("#"))
                    targetUrls = str.substring(1);
                // 其他节点对key的pr贡献值
                else
                    newPR += Float.valueOf(str);
            }
            newPR = newPR * alpha + (1 - alpha);    // pr = pr * alpha + (1 - alpha) / N
            context.write(key, new Text(Float.toString(newPR) + '\t' + targetUrls));
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
