import java.io.IOException;
import java.util.StringTokenizer;
import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class WordSort {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        private String filename;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
                `map' is called each time a line is read from file `filename'
                input: one line from file
                output: < "string:filename", "1" >
            */

            split = (FileSplit)context.getInputSplit();
            filename = new File(split.getPath().toString()).getName();
            System.out.println("\033[0;32mfilename:" + filename + "\033[0m");

            StringTokenizer iter = new StringTokenizer(value.toString());
            while (iter.hasMoreTokens()) {
                keyInfo.set(iter.nextToken() + ":" + filename);
                valueInfo.set("1");
                context.write(keyInfo, valueInfo);
            }
        }
    }

    // combine
    public static class Combine extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
                `Combine.reduce' is called each time we have a set of KV pair < "string:filename", 1 >
                input: `key' is "string:filename", `values' is { 1, 1, ..., 1 }
                output: < "string", "filename:num" >
            */

            int sum = 0;
            for (Text val : values) {
                sum += Integer.parseInt(val.toString());
            }
            
            int index = key.toString().indexOf(":");
            if (index < 0) {
                System.err.println("\033[0;33mError occurred while parsing '" + key.toString() + "', index=" + index + "\033[0m");
                System.exit(-1);
            }

            String strInKey = key.toString().substring(0, index);
            String filenameInKey = key.toString().substring(index + 1);
            Text outKey = new Text(strInKey);
            Text outValue = new Text(filenameInKey + ":" + sum);
            context.write(outKey, outValue);
        }
    }

    // reduce
    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
                `Reduce.reduce' is called when we have checked all files for one string
                input: `key' is "string", `values' is { "file_1:num_1", "file_2:num_2", ..., "file_x:num_x" }
                output: < string, "IDT;tot_num" >
            */
            
            int tot_num = 0;
            int relevant_file_num = 0;

            for (Text val : values) {
                int index = val.toString().indexOf(":");
                if (index < 0) {
                    System.err.println("\033[0;33mError occurred while parsing '" + val.toString() + "', index=" + index + "\033[0m");
                    System.exit(-1);
                }
                int thisNum = Integer.parseInt(val.toString().substring(index + 1));
                if (thisNum <= 0) {
                    System.err.println("\033[0;33mWhat???\033[0m");
                    System.exit(-1);
                } 
                tot_num += thisNum;
                relevant_file_num += 1;
            }

            int file_tot_num = context.getConfiguration().getInt("file_tot_num", 0);
            double idt = Math.log10((double)file_tot_num / (relevant_file_num + 1));
            context.write(key, new Text(String.format("%.2f", idt) + ";" + tot_num));
        }
    }

    // driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Number of received args:" + otherArgs.length);
            System.err.println("Usage: WordSort <input directory path>  <outout directory path>");
            System.exit(2);
        }

        // get the number files present in the input using system commands
        FileSystem hdfs = FileSystem.get(conf);
        final int file_tot_num = hdfs.listStatus(new Path(otherArgs[0])).length;
        System.out.println("\033[0;32mtotal file num:" + file_tot_num + "\033[0m");

        Job job = Job.getInstance(conf, "WordSort");
        job.setJarByClass(WordSort.class);

        job.getConfiguration().setInt("file_tot_num", file_tot_num);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
