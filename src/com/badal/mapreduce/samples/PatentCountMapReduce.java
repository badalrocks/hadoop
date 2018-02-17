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
 * Input: patents.txt.
 * Output: patent and number of sub patents for that patent.
 * 
 * @author badal
 *
 */
public class PatentCountMapReduce
{

    public static class PatentCountMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        private static final IntWritable ONE = new IntWritable(1);

        @Override
        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] values = StringUtils.split(value.toString(), " ");

            context.write(new IntWritable(Integer.valueOf(values[0])), ONE);

        }
    }

    public static class PatentCountReducer extends Reducer<IntWritable, IntWritable, IntWritable, LongWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
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
        Job job = Job.getInstance(conf, "patent_count");

        job.setJarByClass(PatentCountMapReduce.class);
        job.setMapperClass(PatentCountMapper.class);
        job.setReducerClass(PatentCountReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(LongWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
