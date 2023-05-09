import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class WordSort {

    public static class WordSortMap extends Mapper<Object, Text, FloatWritable, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit split = (FileSplit) context.getInputSplit();
            String[] line = value.toString().split("\t");
            assert line.length >= 2;
            context.write(new FloatWritable(-Float.parseFloat(line[1])), new Text(line[0]));
        }
    }

    public static class WordSortReduce extends Reducer<FloatWritable, Text, Text, FloatWritable> {
        @Override
        protected void reduce(FloatWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(val, new FloatWritable(-key.get()));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            System.err.println("Number of received args:" + args.length);
            System.err.println("Usage: WordSort <input directory path>  <outout directory path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "WordSort");
        job.setJarByClass(WordSort.class);

        job.setMapperClass(WordSortMap.class);
        job.setReducerClass(WordSortReduce.class);

        job.setMapOutputKeyClass(FloatWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

