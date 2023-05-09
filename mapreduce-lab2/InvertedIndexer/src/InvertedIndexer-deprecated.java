// Chen Weitian
// 201180009@smail.nju.edu.cn

import java.io.IOException;
import java.io.File;
import java.util.StringTokenizer;
import java.util.HashSet;

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
    public static class InvertedIndexerCombine extends Reducer<Text, Text, Text, Text> {
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
    public static class InvertedIndexerReduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
                `Reduce.reduce' is called when we have checked all files for one string
                input: `key' is "string", `values' is { "file_1:num_1", "file_2:num_2", ..., "file_x:num_x" }
                output: < "string\tfreq", "file_1:num_1;file_2:num_2;...;file_x:num_x" >
            */
            
            String fileList = new String();
            int tot = 0, len = 0;

            // not necessary, just for correctness of map-combine-reduce
            HashSet<String> fileSet = new HashSet<String>();

            for (Text val : values) {
                if (fileSet.contains(val.toString())) {
                    System.err.println("\033[0;33mWhat???\033[0m");
                    System.exit(-1);
                }
                fileSet.add(val.toString());
                
                fileList += val.toString() + ";";
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
                tot += thisNum;
                len += 1;
            }

            String freq = String.format("%.2f", (float)tot / len);
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
        job.setCombinerClass(InvertedIndexerCombine.class);
        job.setReducerClass(InvertedIndexerReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
