package examples.mapreduce;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MergeSort {
	private static int maxValue = Integer.MIN_VALUE;
	private static int index = 0;
	/**
	 * @param args
	 * merge fileA and fileB, removing duplicates
	 */
	// override map func, copy input: 'value' to output: 'key' 
	// Mapper<KeyIn, ValueIn, KeyOut, ValueOut>
	// <KeyIn: 行号, ValueIn: 该行字符串>
	public static class MergeSortMapper extends Mapper<Object, Text, IntWritable, Text> {
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			// 将输入的number作为key, 利用Map自身有序的性质排序
			int number = Integer.parseInt(value.toString());
			context.write(new IntWritable(number), new Text(""));
		}
	}
	
	// override reduce func, copy input: 'key' to output: 'key'
	// Reducer<KeyIn, ValueIn, KeyOut, ValueOut>
	public static class MergeSortReducer extends Reducer<IntWritable, Text, Text, Text> {
		public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			// 对于重复的数字, 位次均相同
			if (key.get() > maxValue) {
				++index;
				maxValue = key.get();
			}
			context.write(new Text(index + " " + key.toString()), new Text(""));
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		String[] otherArgs = new String[] {
			"hdfs://localhost:9000/user/hadoop/exp1/MergeSortInput/input1.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/MergeSortInput/input2.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/MergeSortInput/input3.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/MergeSortOutput"
		};
		if (otherArgs.length != 4) {
			System.err.println("Usage: mergesort <in1> <in2> <in3> <out>");
			System.exit(2);
		}
		
		Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		Job job = Job.getInstance(conf, "MergeSort");
		job.setJarByClass(MergeSort.class);

		job.setMapperClass(MergeSortMapper.class);
		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(MergeSortReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		for (int i = 0; i < otherArgs.length - 1; i++)
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
