package kmeans;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class KMeansIterator {

	public static class KMeansMapper extends Mapper<Object, Text, IntWritable, Text> {
		private List<Point> clusters = new ArrayList<>();
		/**
         * setup: 读入k-means center
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String clusterPath = conf.get("cluster_path");
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(clusterPath))));
            String line = new String();
            while ((line = reader.readLine()) != null)
				clusters.add(new Point(line));

            reader.close();
        }

		protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			Point p = new Point(value.toString());
			double minDistance = Double.MAX_VALUE;
			int k = 0;		// 初始化类为0
			for (int i = 0; i < clusters.size(); i++) {
				double d = p.sub(clusters.get(i)).norm();
				if (d < minDistance) {
					minDistance = d;
					k = i;
				}
			}
			context.write(new IntWritable(k), new Text(p.toString()));
		}

	}

	public static class KMeansReducer extends Reducer<IntWritable, Text, Text, Text> {

		protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int n = 0;
			Point p = new Point();
			for (Text value : values) {
				p = p.add(new Point(value.toString()));
				++n;
			}

			context.write(null, new Text(p.divide(n).toString()));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		conf.set("cluster_path", args[0]);
        Job job = Job.getInstance(conf, "KMeansIterator");
        job.setJarByClass(KMeansIterator.class);

		job.setMapperClass(KMeansMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

		job.setReducerClass(KMeansReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
	}
}
