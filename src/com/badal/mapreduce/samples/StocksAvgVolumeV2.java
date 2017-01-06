package com.badal.mapreduce.samples;

import java.io.IOException;

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

import com.badal.mapreduce.domain.CountAverageTuple;

/**
 * Enhanced version of StocksAvgVolume for calculating average. Computes average using sum(average*count)/sum(count) so
 * that average operation becomes associative and hence combiner can be used. Combiner helps in minimizing network
 * traffic by combining intermediate results of mappers before sending data it to reducers.
 * 
 * @author badal
 *
 */
public class StocksAvgVolumeV2
{

    public static class StocksAvgVolumeV2Mapper extends Mapper<Object, Text, Text, CountAverageTuple>
    {

        private CountAverageTuple outCountAverageTuple = new CountAverageTuple();
        private Text              outCompany           = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, CountAverageTuple>.Context context)
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

            outCompany.set(company);

            outCountAverageTuple.setCount(1);
            outCountAverageTuple.setAverage(volume);

            context.write(outCompany, outCountAverageTuple);

        }

    }

    public static class StocksAvgVolumeV2Reducer extends Reducer<Text, CountAverageTuple, Text, CountAverageTuple>
    {

        @Override
        protected void reduce(Text key, Iterable<CountAverageTuple> values,
                Reducer<Text, CountAverageTuple, Text, CountAverageTuple>.Context context) throws IOException,
                InterruptedException
        {
            long sum = 0;
            long count = 0;

            for (CountAverageTuple value : values)
            {
                sum += value.getCount() * value.getAverage();
                count += value.getCount();
            }

            double average = sum / count; // makes the average associative so that Combiner can be used for
                                          // optimization.

            CountAverageTuple outCountAverageTuple = new CountAverageTuple();
            outCountAverageTuple.setAverage(average);
            outCountAverageTuple.setCount(count);

            context.write(key, outCountAverageTuple);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stocks_avg_vol_v2");

        job.setJarByClass(StocksAvgVolumeV2.class);
        job.setMapperClass(StocksAvgVolumeV2Mapper.class);
        job.setReducerClass(StocksAvgVolumeV2Reducer.class);
        job.setCombinerClass(StocksAvgVolumeV2Reducer.class); // optimization.

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(CountAverageTuple.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.getConfiguration().set("mapreduce.output.basename", "stock_avg_vol_v2");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
