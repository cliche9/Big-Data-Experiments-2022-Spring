package pagerank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class PageRankDriver {
	private static final int loopMax = 10;
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        if (args.length != 2) {
            System.err.println("Usage: PageRank <input> <output>");
            System.exit(2);
        }
        
		String[] forGB = {"", args[1] + "/Data0"};  // GraphBuilder
		forGB[0] = args[0];
		if (hdfs.exists(new Path(forGB[1])))
			hdfs.delete(new Path(forGB[1]), true);
		GraphBuilder.main(forGB);
		
		String[] forItr = {"", ""};  // PageRankIterator
		for (int i = 0; i < loopMax; i++) {
			forItr[0] = args[1] + "/Data" + i;
			forItr[1] = args[1] + "/Data" + (i + 1);
			if (hdfs.exists(new Path(forItr[1])))
				hdfs.delete(new Path(forItr[1]), true);
			PageRankIterator.main(forItr);
		}
		
		String[] forRV = {args[1] + "/Data" + loopMax, args[1] + "/FinalRank/Top10"};  // PageRankViewer
		if (hdfs.exists(new Path(forRV[1])))
			hdfs.delete(new Path(forRV[1]), true);
		PageRankViewer.main(forRV);
	}
}