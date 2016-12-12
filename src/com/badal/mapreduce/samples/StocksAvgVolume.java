package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This program reads a csv file that has stocks data for 1 month and computes
 * the average volume of each stock.
 * 
 * @author badal
 *
 */
public class StocksAvgVolume
{

    public static class StocksAvgVolumeMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");
            String company = stockData[0];

            if (StringUtils.equals(company, "Ticker"))
            {
                // hack to skip header. bad: this if condition will be evaluated
                // for each input line.
                return;
            }

            int volume = Integer.valueOf(stockData[6]);

            context.write(new Text(company), new IntWritable(volume));
        }

    }

    public static class StocksAvgVolumeReducer extends Reducer<Text, IntWritable, Text, DoubleWritable>
    {

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values,
                Reducer<Text, IntWritable, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            long sum = 0;
            int count = 0;

            for (IntWritable value : values)
            {
                sum += value.get();
                count++;
            }

            double average = sum / count;
            context.write(key, new DoubleWritable(average));

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock_vol_avg_job");
        job.setJarByClass(StocksAvgVolume.class);
        job.setMapperClass(StocksAvgVolumeMapper.class);
        job.setReducerClass(StocksAvgVolumeReducer.class);
        // job.setCombinerClass(StocksAvgVolumeReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);
        
        job.getConfiguration().set("mapreduce.output.basename", "stock_avg_vol");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
