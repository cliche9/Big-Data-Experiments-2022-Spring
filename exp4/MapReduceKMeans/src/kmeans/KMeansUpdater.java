package kmeans;

import java.util.List;

public class KMeansUpdater {
    public static class UpdaterMapper extends Mapper<Object, Text, Text, Text> {
		private List<Point> clusters = new ArrayList<>();
		/**
         * setup: 读入k-means center
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String clusterPath = conf.get("cluster_path");
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(clusterPath))));
            String line;
            while ((line = reader.readLine()) != null)
				clusters.add(new Point(line.split("\t")[1]));
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

			context.write(key, new Text(clusters.get(k).toString()));
		}

	}

	public static class UpdaterReducer extends Reducer<IntWritable, Text, Text, Text> {

		protected void reduce(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.write(null, value);
		}
	}

    public static void main(String[] args) {
		Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "KMeansUpdater");
        job.setJarByClass(KMeansUpdater.class);

		job.setMapperClass(UpdaterMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

		job.setReducerClass(UpdaterReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
	}
}
