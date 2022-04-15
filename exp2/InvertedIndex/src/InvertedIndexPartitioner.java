import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexPartitioner {
    public static class InvertedIndexPartitionerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static Set<String> stopWords;
        /**
         * setup: 读入停用词
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordPath = conf.get("stop_words_path");
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(stopWordPath))));
            String word;
            stopWords = new TreeSet<>();
            while ((word = reader.readLine()) != null) 
                stopWords.add(word.trim());
            reader.close();
        }

        /**
         * key: 文本段落相对全文的offset
         * value: 文本段落
         * output:
         *      key: Text, 单词
         *      value: Pair<String, Integer>, Pair.key: word+filename
         */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String fileName = fileSplit.getPath().getName();
            String regex = new String("[a-z0-9]+");
            List<String> words = regexSplit(value.toString().toLowerCase(), regex);
            // wordSet: <word, file#count>
            Map<String, Integer> wordSet = new HashMap<>();
            for (String word : words) {
                if (wordSet.containsKey(word))
                    wordSet.put(word, wordSet.get(word) + 1);
                else
                    wordSet.put(word, 1);
            }
            // output: <word#filename, wordcount>
            for(Map.Entry<String, Integer> entry : wordSet.entrySet()) {
                context.write(new Text(entry.getKey() + "#" + fileName), new IntWritable(entry.getValue()));
            }
        } 

        private List<String> regexSplit(String str, String pattern) {
            Pattern r = Pattern.compile(pattern);
            Matcher m = r.matcher(str);
            List<String> words = new ArrayList<>();
            while (m.find()) {
                String word = m.group();
                if (stopWords.contains(word))
                    continue;
                words.add(word);
            }
            return words;
        }
    }

    /**
     * Combiner: 用于在map过后, reduce之前整合数据, 先一步对重复key求和
     */
    public static class InvertedIndexCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    /**
     * Partitioner: 自定义Shuffle and sort中Shuffle的hash规则, 只使用部分key进行hash操作
     */
    public static class InvertedIndexCustomizedPartitioner extends HashPartitioner<Text, IntWritable> {
        @Override
        public int getPartition(Text key, IntWritable value, int numReduceTasks) {
            String term = key.toString().split("#")[0];
            return super.getPartition(new Text(term), value, numReduceTasks);
        }
    }

    /**
     * Reducer: 取出hash结果, 将相同词、文件名的value进行求和输出
     */
    public static class InvertedIndexPartitionerReducer extends Reducer<Text, IntWritable, Text, Text> {
        private static String prevWord = null;
        private static int prevCount = 0;
        private static String prevFile = null;
        private static StringBuilder outString = new StringBuilder();
        private static int total = 0;
        
        /**
         * key: word#fileName
         * value: key所对应的计数list
         * output:
         *      key: Text, 单词
         *      value: <file1, count>;<file2, count>...<total, count>.
         */
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            String[] infoList = key.toString().split("#");
            if (prevWord == null)
                prevWord = new String(infoList[0]);
            if (prevFile == null)
                prevFile = new String(infoList[1]);
            
            if (!infoList[0].equals(prevWord)) {
                // 遇到不同的word, 需要输出outString, 结束该词
                outString.append("<" + prevFile + "," + prevCount + ">;<total," + total + ">.");
                context.write(new Text(prevWord), new Text(outString.toString()));
                // 重新初始化
                outString.setLength(0);
                prevCount = 0;
                total = 0;
                prevWord = infoList[0];
                prevFile = infoList[1];
            } else if (!infoList[1].equals(prevFile)) {
                // 同一单词, 遇到不同的fileName, 需要将该文件的信息加入outString
                outString.append("<" + prevFile.toString() + "," + prevCount + ">;");
                // 部分重新初始化
                prevCount = 0;
                prevFile = infoList[1];
            }
            for (IntWritable value : values) {
                prevCount += value.get();
                total += value.get();
            }
        }

        /**
         * cleanup: 将最后一组数据输出
         */
        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            outString.append("<" + prevFile + "," + prevCount + ">;<total," + total + ">.");
            context.write(new Text(prevWord), new Text(outString.toString()));
            super.cleanup(context);
        }
    }

    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        if (args.length != 3) {
            System.err.println("Usage: InvertedIndex <stop-words> <input> <output>");
            System.exit(2);
        }
        String stopWordsPath = args[0];
        String inputPath = args[1];
        String outputPath = args[2];
        
        if (hdfs.exists(new Path(outputPath))) {
            hdfs.delete(new Path(outputPath), true);
        }
        conf.setStrings("stop_words_path", stopWordsPath);

        Job job = Job.getInstance(conf, "InvertedIndexCustomizedPartitioner");
        job.setJarByClass(InvertedIndexPartitioner.class);

        job.setMapperClass(InvertedIndexPartitionerMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setCombinerClass(InvertedIndexCombiner.class);
        job.setPartitionerClass(InvertedIndexCustomizedPartitioner.class);

        job.setReducerClass(InvertedIndexPartitionerReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
