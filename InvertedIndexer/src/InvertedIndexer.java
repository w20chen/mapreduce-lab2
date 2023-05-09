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
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class InvertedIndexer {

    public static class InvertedIndexerMap extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        private String filename;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
             * Mapper is called each time a line is read from file
             * input: one line from file
             * output: < string, filename >
             */

            split = (FileSplit) context.getInputSplit();
            filename = new File(split.getPath().toString()).getName();
            System.out.println("\033[0;32mfilename:" + filename + "\033[0m");

            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                keyInfo.set(iter.nextToken());
                valueInfo.set(filename);
                context.write(keyInfo, valueInfo);
            }
        }
    }

    public static class InvertedIndexerReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
             * Reducer is called when we have checked all files for one string
             * input: key is string, values is { file_1, file_2, ..., file_x }
             * output: < string\tfreq, file_1:num_1;file_2:num_2;...;file_x:num_x >
             */

            TreeMap<String, Integer> fileMap = new TreeMap<String, Integer>();

            for (Text val : values) {
                if (!fileMap.containsKey(val.toString())) {
                    fileMap.put(val.toString(), 1);
                }
                else {
                    fileMap.put(val.toString(), fileMap.get(val.toString()) + 1);
                }
            }

            int tot = 0;
            String fileList = new String();
            for (Map.Entry<String, Integer> e : fileMap.entrySet()) {
                fileList = fileList + e.getKey() + ":" + e.getValue() + ";" ;
                tot += e.getValue();
            }

            String freq = String.format("%.2f", (float) tot / fileMap.size());
            context.write(new Text(key.toString() + "\t" + freq), new Text(fileList));
        }
    }

    // driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 2) {
            System.err.println("Number of received args:" + args.length);
            System.err.println("Usage: InvertedIndexer <input directory path>  <outout directory path>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "InvertedIndexer");
        job.setJarByClass(InvertedIndexer.class);

        job.setMapperClass(InvertedIndexerMap.class);
        job.setReducerClass(InvertedIndexerReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
