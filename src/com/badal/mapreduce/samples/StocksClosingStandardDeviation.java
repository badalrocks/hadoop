package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.ArrayList;
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
 * Map reduce program to calculate standard deviation of stock closing price. Mapper outputs company name and stock closing price.
 * Reducer iterates over all values of each company and calculates std deviation using below steps:
 * 1. Calculate average.
 * 2. Calculate difference of each value and average. Square the result.
 * 3. Calculate sum of all results of step 2.
 * 4. Calculate variance: step 3 / count of values.
 * 5. Standard Deviation = Sqrt(Variance).
 * 
 * This is a less efficient version as Combiner is not used.
 * 
 * @author badal
 *
 */
public class StocksClosingStandardDeviation
{

    public static class StocksClosingStandardDeviationMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        Text           outCompany = new Text();
        DoubleWritable outClosing = new DoubleWritable();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, DoubleWritable>.Context context)
                throws IOException, InterruptedException
        {
            String[] companyData = value.toString().split(",");
            String company = companyData[0];

            if (StringUtils.equals(company, "Ticker"))
            {
                // ignore header.
                return;
            }

            double closingPrice = Double.valueOf(companyData[5]);

            outCompany.set(company);
            outClosing.set(closingPrice);

            context.write(outCompany, outClosing);

        }

    }

    public static class StocksClosingStandardDeviationReducer extends
            Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {

        DoubleWritable outStdDeviation = new DoubleWritable();

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            double sum = 0;
            int count = 0;
            List<Double> cache = new ArrayList<Double>();

            for (DoubleWritable dw : values)
            {
                double val = dw.get();
                sum += val;
                count++;

                cache.add(val); // needed to iterate again.
            }

            double average = sum / count;

            double sumSquaredDiff = 0;

            for (Double d : cache)
            {
                double squaredDiff = Math.pow(d - average, 2);
                sumSquaredDiff += squaredDiff;
            }

            double variance = sumSquaredDiff / count;

            double stdDeviation = Math.sqrt(variance);

            outStdDeviation.set(stdDeviation);

            context.write(key, outStdDeviation);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "closing_std_dev");

        job.setJarByClass(StocksClosingStandardDeviation.class);
        job.setMapperClass(StocksClosingStandardDeviationMapper.class);
        job.setReducerClass(StocksClosingStandardDeviationReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
