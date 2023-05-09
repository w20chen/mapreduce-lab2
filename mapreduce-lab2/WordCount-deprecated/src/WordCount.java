// Chen Weitian
// 201180009@smail.nju.edu.cn

import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordCount {

    public static class WordCountMap extends Mapper<Object, Text, Text, IntWritable> {
        private Text keyInfo = new Text();
        private FileSplit split;
        private final IntWritable one = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            split = (FileSplit) context.getInputSplit();

            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                keyInfo.set(iter.nextToken());
                context.write(keyInfo, one);
            }
        }
    }

    public static class WordCountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

    // driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            System.err.println("Number of received args:" + args.length);
            System.err.println("Usage: WordCount <input directory path>  <outout directory path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "WordCount");
        job.setJarByClass(WordCount.class);

        job.setMapperClass(WordCountMap.class);
        job.setCombinerClass(WordCountReduce.class);
        job.setReducerClass(WordCountReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

