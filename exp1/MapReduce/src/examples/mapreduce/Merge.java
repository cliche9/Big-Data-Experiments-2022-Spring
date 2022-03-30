package examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Merge {
	/**
	 * @param args
	 * merge fileA and fileB, removing duplicates
	 */
	// override map func, copy input: 'value' to output: 'key' 
	// Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
	public static class MergeMapper extends Mapper<Object, Text, Text, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 将输入的value作为key, 利用Map自身的性质去重
			context.write(new Text(value), new Text(""));
		}
	}
	
	// override reduce func, copy input: 'key' to output: 'key'
	// Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
	public static class MergeReducer extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			context.write(key, new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new String[] {
			"hdfs://localhost:9000/user/hadoop/exp1/MergeInput/inputA.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/MergeInput/inputB.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/MergeOutput"
		};
		if (otherArgs.length != 3) {
			System.err.println("Usage: merge <in1> <in2> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Merge and remove duplicates");
		job.setJarByClass(Merge.class);
		job.setMapperClass(MergeMapper.class);
		job.setCombinerClass(MergeReducer.class);
		job.setReducerClass(MergeReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(otherArgs[0]), new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}
