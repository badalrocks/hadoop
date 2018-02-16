package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
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

/**
 * Input: alphabets.txt.
 * Output: Calculate size of each word and count the number of words of that size. 
 * @author badal
 *
 */
public class AlphabetCountMapReduce
{

    public static class AlphabetCountMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] values = StringUtils.split(value.toString(), " ");

            for (String val : values)
            {
                context.write(new IntWritable(StringUtils.length(val)), ONE);
            }

        }

    }

    public static class AlphabetCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable>
    {
        @Override
        protected void reduce(IntWritable key, Iterable<IntWritable> values,
                Reducer<IntWritable, IntWritable, IntWritable, LongWritable>.Context context) throws IOException,
                InterruptedException
        {
            long count = 0;

            for (IntWritable val : values)
            {
                count++;
            }

            context.write(key, new LongWritable(count));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "alphabet_size");

        job.setJarByClass(AlphabetCountMapReduce.class);
        job.setMapperClass(AlphabetCountMapper.class);
        job.setReducerClass(AlphabetCountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "alphabet_size");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
