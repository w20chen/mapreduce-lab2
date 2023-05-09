// Chen Weitian
// 201180009@smail.nju.edu.cn

import java.util.Map;
import java.util.TreeMap;
import java.util.StringTokenizer;
import java.util.ArrayList;
import java.util.Iterator;
import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;


public class TFIDF {

    public static class TFIDFMapJob1 extends Mapper<Object, Text, Text, Text> {
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

            split = (FileSplit)context.getInputSplit();
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

    public static class TFIDFReduceJob1 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /*
             * Reducer is called when we have checked all files for one string
             * input: key is string, values is { file_1, file_2, ..., file_x }
             * output: { < filename\tstring, num_in_file\tIDF > }
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

            int str_tot_num = 0;
            int relevant_file_num = fileMap.size();
            System.out.println("\033[0;32m" + key.toString() + "\trelevant file num:" + relevant_file_num + "\033[0m");
            for (Map.Entry<String, Integer> e : fileMap.entrySet()) {
                str_tot_num += e.getValue();
            }

            int file_tot_num = context.getConfiguration().getInt("file_tot_num", 0);
            double IDF = Math.log10((double)file_tot_num / (relevant_file_num + 1));

            // String IDF_str = String.format("%.2f", IDF);
            String IDF_str = "" + IDF;

            for (Map.Entry<String, Integer> e : fileMap.entrySet()) {
                context.write(
                    new Text(e.getKey() + "\t" + key.toString()), 
                    new Text(e.getValue() + "\t" + IDF_str)
                );
            }
        }
    }

    public static class TFIDFMapJob2 extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text();
        private Text valueInfo = new Text();
        private FileSplit split;
        private String filename;

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            /*
             * input: one line from file: filename, string, num_in_file, IDF
             * output: < filename, < string, num_in_file, IDF > >
             */

            String[] line = value.toString().split("\t");
            if (line.length != 4) {
                System.exit(-1);
            }
            context.write(
                new Text(line[0]),
                new Text(line[1] + "\t" + line[2] + "\t" + line[3])
            );
        }
    }

    public static class TFIDFReduceJob2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int tot_str_num_in_file = 0;
            ArrayList<Text> no_idea_why = new ArrayList();

            for (Text val : values) {
                tot_str_num_in_file += Integer.parseInt(val.toString().split("\t")[1]);
                no_idea_why.add(new Text(val));
            }

            for (Text v : no_idea_why) {
                String[] entry = v.toString().split("\t");
                String filename = key.toString();
                String str = entry[0];
                double tf = (double)Integer.parseInt(entry[1]) / tot_str_num_in_file;
                double IDF = Double.parseDouble(entry[2]);
                double tf_idt = tf * IDF;
                // String tf_idt_str =  String.format("%.2f", tf_idt);
                String tf_idt_str =  "" + tf_idt;

                context.write(
                    new Text(filename + "\t" + str),
                    new Text(tf_idt_str)
                );
            }
        }
    }

    // driver
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        args = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (args.length != 3) {
            System.err.println("Number of received args:" + args.length);
            System.err.println("Usage: TFIDF <input path> <intermediate path> <outout path>");
            System.exit(2);
        }

        // get the number files present in the input using system commands
        FileSystem hdfs = FileSystem.get(conf);
        final int file_tot_num = hdfs.listStatus(new Path(args[0])).length;
        System.out.println("\033[0;32mtotal file num:" + file_tot_num + "\033[0m");

        String input_dir = args[0];
        String intermediate_dir = args[1];
        String output_dir = args[2];
        String intermediate_file = intermediate_dir;
        if (intermediate_file.charAt(intermediate_file.length() - 1) != '/') {
            intermediate_file += "/";
        }
        intermediate_file += "part-r-00000";

        /* 1 */
        System.out.println("\033[0;32mStart job1\033[0m");
        Job job1 = Job.getInstance(conf, "TF-IDF Job1");
        job1.setJarByClass(TFIDF.class);

        job1.getConfiguration().setInt("file_tot_num", file_tot_num);

        job1.setMapperClass(TFIDFMapJob1.class);
        job1.setReducerClass(TFIDFReduceJob1.class);

        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job1, new Path(input_dir));
        FileOutputFormat.setOutputPath(job1, new Path(intermediate_dir));

        if (job1.waitForCompletion(true) == false) {
            System.exit(-1);
        }
        System.out.println("\033[0;32mJob1 is completed\033[0m");

        /* 2 */
        System.out.println("\033[0;32mStart job2\033[0m");
        Job job2 = Job.getInstance(conf, "TF-IDF Job2");
        job2.setJarByClass(TFIDF.class);

        job2.getConfiguration().setInt("file_tot_num", file_tot_num);

        job2.setMapperClass(TFIDFMapJob2.class);
        job2.setReducerClass(TFIDFReduceJob2.class);

        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job2, new Path(intermediate_file));
        FileOutputFormat.setOutputPath(job2, new Path(output_dir));

        if (job2.waitForCompletion(true) == false) {
            System.exit(-1);
        }
	    System.out.println("\033[0;32mJob2 is completed\033[0m");
        System.exit(0);
    }
}
