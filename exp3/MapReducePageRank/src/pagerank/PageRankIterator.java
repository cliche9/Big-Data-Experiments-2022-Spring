import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.swing.text.AbstractDocument.Content;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * PageRankIterator: 迭代计算PR值
 */
public class PageRankIterator {

    private static float alpha = 0.85f;
    
    public static class PageRankMapper extends Mapper<Text, Text, Text, Text> {

        /**
         * Input
         *  <Url, PR + TargetUrls>
         * Output
         *  <Url, TargetUrls>, <TargetUrl, PR>
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(Text key, Text value, Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {
            String[] targetUrls = value.toString().split(',');
            float prValue = Float.valueOf(targetUrls[0]);
            int n = targetUrls.length - 1;
            StringBuilder urls;
            for (int i = 1; i < targetUrls.length; i++) {
                context.write(new Text(targetUrls[i]), new Text(prValue / n));
                urls.append(targetUrls[i]).append(',');
            }
            context.write(key, new Text(urls.deleteCharAt(urls.length() - 1).toString());
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
         * @throws IndexOutOfBoundsException
         */
        protected void reduce(Text key, Text values, Context context) throws IOException, InterruptedException, IndexOutOfBoundsException {
            
        }
    }
}
