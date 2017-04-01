package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Distinct pattern: This MapReduce program outputs distinct ticker symbol from stock data csv file. Mapper output key
 * is ticker symbol and value is NullWritable object. Reducer output key is distinct ticker symbol and value is
 * NullWritable object.
 * 
 * @author badal
 *
 */
public class DistinctStockMapReduce
{
    public static class DistinctStockMapReduceMapper extends Mapper<Object, Text, Text, NullWritable>
    {
        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, NullWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");
            String ticker = stockData[0];

            if (StringUtils.equals(ticker, "Ticker"))
            {
                return;
            }

            context.write(new Text(ticker), NullWritable.get());
        }

    }

    public static class DistinctStockMapReduceReducer extends Reducer<Text, NullWritable, Text, NullWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values,
                Reducer<Text, NullWritable, Text, NullWritable>.Context context) throws IOException,
                InterruptedException
        {

            context.write(key, NullWritable.get());

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct_stock_ticker");

        job.setJarByClass(DistinctStockMapReduce.class);
        job.setMapperClass(DistinctStockMapReduceMapper.class);
        job.setReducerClass(DistinctStockMapReduceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
