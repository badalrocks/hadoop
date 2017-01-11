package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
 * Map reduce program to calculate median of stock closing price. Mapper outputs company name and closing price. Reducer
 * sorts list of closing price for each stock and calculates median depending on even or odd list size.
 * 
 * @author badal
 *
 */
public class StocksMedianClosing
{
    public static class StocksMedianClosingMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        Text           outKey   = new Text();
        DoubleWritable outValue = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] stockData = value.toString().split(",");
            String companyData = stockData[0];

            // skip header
            if (StringUtils.equals(companyData, "Ticker"))
            {
                return;
            }

            double closingPrice = Double.valueOf(stockData[5]);

            outKey.set(companyData);
            outValue.set(closingPrice);

            context.write(outKey, outValue);
        }

    }

    public static class StocksMedianClosingReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {

        DoubleWritable result;

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            List<Double> allValues = new ArrayList<Double>();

            for (DoubleWritable dw : values)
            {
                allValues.add(dw.get());
            }

            Collections.sort(allValues); // for calculating median

            int size = allValues.size();

            if (size % 2 == 0)
            {
                Double m1 = allValues.get(size / 2 - 1);
                Double m2 = allValues.get(size / 2);
                result = new DoubleWritable((m1 + m2) / 2);
            }
            else
            {
                result = new DoubleWritable(allValues.get(size / 2));

            }

            context.write(key, result);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stocks_median_closing");

        job.setJarByClass(StocksMedianClosing.class);
        job.setMapperClass(StocksMedianClosingMapper.class);
        job.setReducerClass(StocksMedianClosingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "stocks_median_closing");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
