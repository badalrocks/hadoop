package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Input: Temperature.txt.
 * Output: Max temperature by year.
 * 
 * @author badal
 *
 */
public class MaxTemperatureMapReduce
{
    public static class MaxTemperatureMapper extends Mapper<Object, Text, IntWritable, IntWritable>
    {
        @Override
        public void map(Object key, Text value, Mapper<Object, Text, IntWritable, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] values = StringUtils.split(value.toString(), " ");
            int year = Integer.valueOf(values[0]);
            int temperature = Integer.valueOf(values[1]);

            context.write(new IntWritable(year), new IntWritable(temperature));

        }
    }

    public static class MaxTemperatureReducer extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable>
    {
        @Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
                Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) throws IOException,
                InterruptedException
        {
            int maxTemp = Integer.MIN_VALUE;

            for (IntWritable val : values)
            {
                if (val.get() > maxTemp)
                {
                    maxTemp = val.get();
                }
            }

            context.write(key, new IntWritable(maxTemp));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "max_temperature");

        job.setJarByClass(MaxTemperatureMapReduce.class);
        job.setMapperClass(MaxTemperatureMapper.class);
        job.setReducerClass(MaxTemperatureReducer.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "max_temperature");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
