package kmeans;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class KMeansDriver {
	private static final int loopMax = 10;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        if (args.length != 2) {
            System.err.println("Usage: KMeans <input> <output>");
            System.exit(2);
        }
		
        /**
		String[] forItr = {"", "", ""};             // KMeansIterator
		for (int i = 0; i < loopMax; i++) {
			forItr[0] = args[1] + "/Cluster_" + i + "/part-r-00000";
			forItr[1] = args[1] + "/Points";
			forItr[2] = args[1] + "/Cluster_" + (i + 1);
			System.out.println(forItr[2]);
			if (hdfs.exists(new Path(forItr[2])))
				hdfs.delete(new Path(forItr[2]), true);
			KMeansIterator.main(forItr);
		}
        */
        String[] forUpd = {"", "", ""};
		forUpd[0] = args[1] + "/Cluster_" + loopMax + "/part-r-00000";
        forUpd[1] = args[1] + "/Points";
		forUpd[2] = args[1] + "/Points_Clustered";
        if (hdfs.exists(new Path(forUpd[2])))
            hdfs.delete(new Path(forUpd[2]), true);
        KMeansUpdater.main(forUpd);
	}
}