import java.io.*;
import java.net.URI;
import java.util.regex.Pattern;
import java.util.HashSet;
import java.util.Set;
import java.io.IOException;
import java.util.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.StringUtils;

public class Count {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{    // TokenizerMapper 继承 Mapper 父类
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                word.set(value.toString());
                context.write(word,one);
        }
    }
    public static class SortReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
        //定义treeMap来保持统计结果,由于treeMap是按key升序排列的,这里要人为指定Comparator以实现倒排
        //这里先使用统计数为key，被统计的单词为value
        private TreeMap<Integer, String> treeMap = new TreeMap<Integer, String>(new Comparator<Integer>() {
            @Override
            public int compare(Integer x, Integer y) {
                return y.compareTo(x);
            }
        });
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //reduce后的结果放入treeMap,而不是向context中记入结果
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (treeMap.containsKey(sum)) {  //具有相同单词数的单词之间用逗号分隔
                String value = treeMap.get(sum) + "," + key.toString();
                treeMap.put(sum, value);
            } else {
                treeMap.put(sum, key.toString());
            }
        }
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //将treeMap中的结果,按value-key顺序写入context中
            for (Integer key : treeMap.keySet()) {
                if (treeMap.get(key).toString().indexOf(",")!=-1) { // 说明有，有同样个数的单词
                    String[] splitstr=treeMap.get(key).toString().split(",");
                    for (int i=0;i<splitstr.length;++i){
                            context.write(new Text(splitstr[i]), new IntWritable(key));
                    }
                }
                else{
                    String s = treeMap.get(key);
                    context.write(new Text(s),new IntWritable(key));
                }
            }
        }
    }
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        job.setJarByClass(Count.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(SortReducer.class);
        job.setReducerClass(SortReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.waitForCompletion(true);
    }
}