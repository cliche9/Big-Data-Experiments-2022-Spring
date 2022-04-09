import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndex {    
    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        private static Set<String> stopWords = new TreeSet<>();
        /**
         * setup: 读入停用词
         */
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Configuration conf = context.getConfiguration();
            String stopWordPath = conf.get("stop_words_path");
            BufferedReader reader = new BufferedReader(new InputStreamReader(FileSystem.get(conf).open(new Path(stopWordPath))));
            String word;
            while ((word = reader.readLine()) != null) 
                stopWords.add(word.trim());
            
            reader.close();
        }
        
        /**
         * key: 文本段落相对全文的offset
         * value: 文本段落
         * output:
         *      key: Text, 单词
         *      value: Pair<String, Integer>, 
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
            // output
            for(Map.Entry<String, Integer> entry : wordSet.entrySet()) {
                context.write(new Text(entry.getKey()), new Text(fileName + "#" + entry.getValue().toString()));
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

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
        /**
         * key: map ===> word
         * values: List< Pair<fileName, count> >
         */
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuilder stringBuilder = new StringBuilder();
            int total = 0;
            Map<String, Integer> fileCount = new TreeMap<>();
            for (Text value : values) {
                String[] infoList = value.toString().split("#");
                if (fileCount.containsKey(infoList[0]))
                    fileCount.put(infoList[0], fileCount.get(infoList[0]) + Integer.parseInt(infoList[1]));
                else 
                    fileCount.put(infoList[0], Integer.parseInt(infoList[1]));
            }
            for (Map.Entry<String, Integer> entry : fileCount.entrySet()) {
                stringBuilder.append("<" + entry.getKey() + "," + entry.getValue().toString() + ">;");
                total += entry.getValue();
            }
            stringBuilder.append("<total," + Integer.toString(total) + ">.");
            context.write(key, new Text(stringBuilder.toString()));
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

        Job job = Job.getInstance(conf, "InvertedIndex");
        job.setJarByClass(InvertedIndex.class);

        job.setMapperClass(InvertedIndexMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
