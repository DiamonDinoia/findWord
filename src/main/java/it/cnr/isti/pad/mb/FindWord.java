package it.cnr.isti.pad.mb;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class FindWord {

    private static String[] words;

    public static class Mymapper extends Mapper<LongWritable, Text, LongWritable, Text>{
        ArrayList<String> lineWords = new ArrayList<>();
        public void map(LongWritable key, Text line, Context context) throws IOException, InterruptedException {
            Collections.addAll(lineWords, line.toString().split("\\s+"));
            for (String word : words) {
                if (lineWords.contains(word)){
                    line.set(line.toString().toUpperCase());
                    context.write(key,line);
                    break;
                }
            }
            lineWords.clear();
        }
    }

    public static class Myreducer extends Reducer<LongWritable,Text, LongWritable, Text>{

        public void reduce(LongWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                context.write(key,value);
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        words = new String[args.length-2];
        System.arraycopy(args, 2, words, 2, args.length - 2);

        Configuration conf = new Configuration();
        Job job = new Job(conf, "findword");

        job.setMapperClass(Mymapper.class);
        job.setReducerClass(Myreducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);

    }
}
