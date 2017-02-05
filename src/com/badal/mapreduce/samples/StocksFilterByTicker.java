package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is filtering example. The program reads the input data from stocks_data.csv and selects only those records where
 * ticker symbol is GOOGL. This is a map-only job. As a result, the output filename will have a part-m prefix instead of
 * part-r. Since the input data does not need to be counted or aggregated, no reducer is required. The program
 * explicitly sets the number of reducer to 0. This is needed because by default number of reducer is set to 1; which
 * leads to Identity Reducer getting kicked in.
 * 
 * @author badal
 *
 */
public class StocksFilterByTicker
{
    public static class StocksFilterByTickerMapper extends Mapper<Object, Text, NullWritable, Text>
    {
        private String tickerFilter;

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, Text>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");

            String ticker = stockData[0];

            if (StringUtils.equals(ticker, "Ticker"))
            {
                return;
            }

            if (StringUtils.equals(ticker, tickerFilter))
            {
                context.write(NullWritable.get(), value);
            }

        }

        @Override
        protected void setup(Mapper<Object, Text, NullWritable, Text>.Context context) throws IOException,
                InterruptedException
        {
            tickerFilter = context.getConfiguration().get("ticker.filter");
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        conf.set("ticker.filter", "GOOGL");

        Job job = Job.getInstance(conf, "stocks_filter_by_ticker");

        job.setJarByClass(StocksFilterByTicker.class);
        job.setMapperClass(StocksFilterByTickerMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setNumReduceTasks(0); // by default it's 1. so identity reducer kicks in.

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
