package pagerank;

public class PageRankDriver {
	private static final int loopMax = 10;
	
	public static void main(String[] args) throws Exception {
		String[] forGB = {"", args[1] + "/Data0"};  // GraphBuilder
		forGB[0] = args[0];
		GraphBuilder.main(forGB);
		
		String[] forItr = {"", ""};  // PageRankIterator
		for (int i = 0; i < loopMax; i++) {
			forItr[0] = args[1] + "/Data" + i;
			forItr[1] = args[1] + "/Data" + (i + 1);
			PageRankIterator.main(forItr);
		}
		
		String[] forRV = {args[1] + "/Data" + loopMax, args[1] + "/FinalRank"};  // PageRankViewer
		PageRankViewer.main(forRV);
	}
}