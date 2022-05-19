package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeansDriver {
	private static final int loopMax = 10;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        if (args.length != 3) {
            System.err.println("Usage: KMeans <cluster> <input> <output>");
            System.exit(2);
        }
		
		String[] forItr = {"", ""};             // KMeansIterator
        String[] forUpd = {"", ""};
		for (int i = 0; i < loopMax; i++) {
			forItr[0] = args[2] + "/Cluster_" + i;
			forItr[1] = args[2] + "/Cluster_" + (i + 1);
			if (hdfs.exists(new Path(forItr[1])))
				hdfs.delete(new Path(forItr[1]), true);
			KMeansIterator.main(forItr);

            forUpd[0] = args[2] + "/Points_" + i;
            forUpd[1] = args[2] + "/Points_" + (i + 1);
            if (hdfs.exists(new Path(forUpd[1])))
                hdfs.delete(new Path(forUpd[1]), true);
            KMeansUpdater.main(forUpd);
		}
	}
}