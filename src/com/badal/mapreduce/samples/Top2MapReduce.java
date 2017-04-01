package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeSet;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Top N pattern. This program output top 2 closing price for each stock. Here N is hard-coded to 2; but it can be
 * configured in properties file. Mapper outputs it's individual top 2 closing price per stock: so it writes 2 key-value
 * pair to the context. Reducer gets top 2 closing price per stock from all the mappers and determines top 2 closing
 * price per stock across all the mappers output.
 * 
 * @author badal
 *
 */
public class Top2MapReduce
{
    public static class Top2MapReduceMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        Map<String, TreeSet<Double>> tickerClosingPrices = new HashMap<String, TreeSet<Double>>();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");
            String ticker = stockData[0];

            if (StringUtils.equals(ticker, "Ticker"))
            {
                return;
            }

            double closingPrice = Double.valueOf(stockData[5]);

            TreeSet<Double> top2ClosingPrice = tickerClosingPrices.get(ticker);

            if (top2ClosingPrice != null)
            {
                top2ClosingPrice.add(closingPrice);

                if (top2ClosingPrice.size() > 2)
                {
                    top2ClosingPrice.remove(top2ClosingPrice.first());
                }
            }
            else
            {
                top2ClosingPrice = new TreeSet<Double>();
                top2ClosingPrice.add(closingPrice);

                tickerClosingPrices.put(ticker, top2ClosingPrice);
            }

        }

        @Override
        protected void cleanup(Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            for (Map.Entry<String, TreeSet<Double>> entry : tickerClosingPrices.entrySet())
            {
                String ticker = entry.getKey();
                TreeSet<Double> top2ClosingPrice = entry.getValue();

                for (Double closing : top2ClosingPrice)
                {
                    context.write(new Text(ticker), new DoubleWritable(closing));
                }
            }
        }

    }

    public static class Top2MapReduceReducer extends Reducer<Text, DoubleWritable, Text, Text>
    {
        Map<String, TreeSet<Double>> tickerClosingPrice = new HashMap<String, TreeSet<Double>>();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException, InterruptedException
        {
            TreeSet<Double> closingPrice = tickerClosingPrice.get(key);

            for (DoubleWritable value : values)
            {
                if (closingPrice != null)
                {
                    closingPrice.add(value.get());

                    if (closingPrice.size() > 2)
                    {
                        closingPrice.remove(closingPrice.first());
                    }
                }
                else
                {
                    closingPrice = new TreeSet<Double>();
                    closingPrice.add(value.get());
                    tickerClosingPrice.put(key.toString(), closingPrice);

                }
            }
        }

        @Override
        protected void cleanup(Reducer<Text, DoubleWritable, Text, Text>.Context context) throws IOException,
                InterruptedException
        {
            for (Map.Entry<String, TreeSet<Double>> entry : tickerClosingPrice.entrySet())
            {
                context.write(new Text(entry.getKey()), new Text(entry.getValue().toString()));
            }
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "top_2_closing_by_ticker");

        job.setJarByClass(Top2MapReduce.class);
        job.setMapperClass(Top2MapReduceMapper.class);
        job.setReducerClass(Top2MapReduceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
