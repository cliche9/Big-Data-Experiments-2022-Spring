package examples.mapreduce;

import java.io.IOException;
import java.util.List;
import java.util.ArrayList;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class DataMining {
	public static class ChildParentMapper extends Mapper<Object, Text, Text, Text> {
		Text outKey = new Text();
		Text outValue = new Text();

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String str = value.toString();
			if (!str.contains("child")) {
				// 除了第一行标记行之外，每一个父子辈分关系，生成两个键值对
				// 第一个：以子辈为key,以父辈加上"p_"作为值
				String[] vals = str.split(" ");
				outKey.set(vals[0]);
				outValue.set("p_" + vals[1]);
				context.write(outKey, outValue);
				// 第二个：以父辈作为key，以"c_"加上子辈作为值
				outKey.set(vals[1]);
				outValue.set("c_" + vals[0]);
				context.write(outKey, outValue);
				// 这两个键值对的作用就是：对上有老下有小的人就会有不止一个的值，这可以寻找出祖孙辈关系
			} else {
				outKey.set("A_title");
				outValue.set("");
				context.write(outKey, outValue);
			}
		}
	}

	public static class ChildParentReducer extends Reducer<Text, Text, Text, Text> {
		List<String> childs = new ArrayList<String>();
		List<String> parents = new ArrayList<String>();


		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String keyString = key.toString();
			childs.clear();
			parents.clear();
			// 如果发现是标题，则写出对应标题
			if (keyString.equals("A_title")) {
				context.write(new Text("grandchild\t"), new Text("grandparent"));
			} else {
				for (Text value : values) {
					String s = value.toString();
					String tag = s.substring(0, 1);		// 取辈分标志
					String name = s.substring(2);		// 取人名
					// 根据首字母为“p”或“c”判断辈分关系
					if (tag.equals("p"))
						parents.add(name);
					else
						childs.add(name);
				}
				// 获取祖辈关系
				for (String child : childs) {
					for (String parent : parents) {
						context.write(new Text(child + "\t\t"), new Text(parent));
					}
				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.get(conf);
		String[] otherArgs = new String[] {
			"hdfs://localhost:9000/user/hadoop/exp1/Child-ParentInput/input.txt",
			"hdfs://localhost:9000/user/hadoop/exp1/Child-ParentOutput"
		};

		Path outputPath = new Path(otherArgs[otherArgs.length - 1]);
		if (hdfs.exists(outputPath)) {
			hdfs.delete(outputPath, true);
		}

		if (otherArgs.length != 2) {
			System.err.println("Usage: child-parent <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(conf, "Child-Parent");
		job.setJarByClass(DataMining.class);

		job.setMapperClass(ChildParentMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(ChildParentReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		for (int i = 0; i < otherArgs.length - 1; i++)
			FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));

		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
