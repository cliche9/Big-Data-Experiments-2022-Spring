package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;


public class PageRankViewer {

    public static class ViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {

        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] tmp = value.toString().split("\t");
            context.write(new DoubleWritable(Double.valueOf(tmp[1])), new Text(tmp[0]));
        }
    }
    
    public static class DescDoubleComparator extends DoubleWritable.Comparator {

        public double compare(WritableComparator cmp, WritableComparable<DoubleWritable> value) {
            return -super.compare(cmp, value);
        }
        
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -super.compare(b1, s1, l1, b2, s2, l2);
        }
    }

    public static class ViewerReducer extends Reducer<DoubleWritable, Text, Text, Text> {
        
        protected void reduce(DoubleWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(new Text("(" + value + ", " + String.format("%.10f", key.get()) + ")"), null);
            }    
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "PageRankViewer");
        job.setJarByClass(PageRankViewer.class);

        job.setMapperClass(ViewerMapper.class);
        job.setMapOutputKeyClass(DoubleWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setSortComparatorClass(DescDoubleComparator.class);

        job.setReducerClass(ViewerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}
