package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
 * is NullWritable object and value is ticker symbol. Reducer output key is NullWritable object and value is distinct
 * ticker symbol.
 * 
 * @author badal
 *
 */
public class DistinctStockMapReduce
{
    public static class DistinctStockMapReduceMapper extends Mapper<Object, Text, NullWritable, Text>
    {
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

            context.write(NullWritable.get(), new Text(ticker));
        }

    }

    public static class DistinctStockMapReduceReducer extends Reducer<NullWritable, Text, NullWritable, Text>
    {
        @Override
        protected void reduce(NullWritable key, Iterable<Text> values,
                Reducer<NullWritable, Text, NullWritable, Text>.Context context) throws IOException,
                InterruptedException
        {
            Set<String> distinctValues = new HashSet<String>();

            for (Text value : values)
            {
                distinctValues.add(value.toString());

            }

            for (String distinctVal : distinctValues)
            {
                context.write(NullWritable.get(), new Text(distinctVal));
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "distinct_stock_ticker");

        job.setJarByClass(DistinctStockMapReduce.class);
        job.setMapperClass(DistinctStockMapReduceMapper.class);
        job.setReducerClass(DistinctStockMapReduceReducer.class);

        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
