package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.CounterGroup;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This is counter example. The benefit of counters is all the counting can be done in map phase. The number of custom
 * counter should typically be less than 100. The output will not be written to a file by framework; it will be
 * available in memory to be written to console or file system.
 * 
 * @author badal
 *
 */
public class StockTickerCounter
{
    public static class StockTickerCounterMapper extends Mapper<Object, Text, NullWritable, NullWritable>
    {

        public static final String TICKER_COUNTER_GRUOP = "Ticker";
        public static final String UNKNOWN_COUNTER      = "Unknown";

        // private String[] tickersArray = {"GOOG", "T", "MSFT", "AMZN"};
        private String[]           tickersArray         = { "GOOGL", "T", "MSFT" };

        private Set<String>        tickers              = new HashSet<String>(Arrays.asList(tickersArray));

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, NullWritable, NullWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");

            String ticker = stockData[0];
            
            if (StringUtils.equals(ticker, "Ticker"))
            {
                return;
            }

            if (tickers.contains(ticker))
            {
                context.getCounter(TICKER_COUNTER_GRUOP, ticker).increment(1);
            }
            else
            {
                context.getCounter(TICKER_COUNTER_GRUOP, UNKNOWN_COUNTER).increment(1);
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stock_ticker_counter");

        job.setJarByClass(StockTickerCounter.class);
        job.setMapperClass(StockTickerCounterMapper.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "stock_ticker_counter");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int result = job.waitForCompletion(true) ? 0 : 1;

        if (result == 0)
        {
            CounterGroup counterGroup = job.getCounters().getGroup(StockTickerCounterMapper.TICKER_COUNTER_GRUOP);

            for (Counter counter : counterGroup)
            {
                System.out.println(counter.getDisplayName() + " , " + counter.getValue());
            }

        }

        // delete the output dir as they won't have any content.
        FileSystem.get(conf).delete(new Path(args[1]), true);

        System.exit(result);

    }

}
