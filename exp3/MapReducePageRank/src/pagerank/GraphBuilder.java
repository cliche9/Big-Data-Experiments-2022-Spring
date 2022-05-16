import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * GraphBuilder: 建立网页链接关系图
 */
public class GraphBuilder {
    private static float initPR = 0.1f;

    public static class GraphBuilderMapper extends Mapper<Object, Text, Text, Text> {
        /**
         * Input
         *  key: 行偏移量
         *  value: 该行对应的文本信息
         * Output
         *  key: 源网站链接
         *  value: 目标网站链接列表
         * @param key
         * @param value
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] Urls = value.split('\t');
            String sourceUrl = Urls[0];
            String targetUrls = Urls[1];
            context.write(new Text(sourceUrl), new Text(Float.toString(initPR) + ',' + targetUrls));
        }
    }

    public static class GraphBuilderReducer extends Reducer<Text, Text, Text, Text> {
        protected void reduce(Text key, Text values, Context context) throws IOException, InterruptedException {
            context.write(key, values);
        }
    }
}