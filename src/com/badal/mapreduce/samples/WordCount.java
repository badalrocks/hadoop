package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is the word count example aka hello world of hadoop. This program reads input from sachin_wiki.txt and outputs
 * number of occurrences for each word.
 * 
 * @author badal
 *
 */
public class WordCount
{

    public static class WordCountMapper extends Mapper<Object, Text, Text, LongWritable>
    {
        Text         outKey   = new Text();
        LongWritable outValue = new LongWritable(1);

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, LongWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] values = value.toString().split(" ");

            for (String v : values)
            {
                outKey.set(v);

                context.write(outKey, outValue);
            }

        }

    }

    public static class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable>
    {
        LongWritable result = new LongWritable();

        @Override
        protected void reduce(Text key, Iterable<LongWritable> values,
                Reducer<Text, LongWritable, Text, LongWritable>.Context context) throws IOException,
                InterruptedException
        {
            long count = 0;
            for (LongWritable val : values)
            {
                count++;
            }

            result.set(count);

            context.write(key, result);
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word_count");

        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "word_count");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
