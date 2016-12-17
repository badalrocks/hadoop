package com.badal.mapreduce.samples;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.badal.mapreduce.domain.StocksMinMaxClosingVo;

/**
 * This program reads the csv file containing stock data and compuetes min and max at the same time. Uses
 * StocksMinMaxClosingVo object to store min and max.
 * 
 * @author badal
 *
 */
public class StocksMinMaxClosing
{
    public static class StocksMinMaxClosingMapper extends Mapper<Object, Text, Text, StocksMinMaxClosingVo>
    {
        private Double                stockClosing;
        private String[]              stockData;
        private String                ticker;
        private Text                  company               = new Text();
        private StocksMinMaxClosingVo stocksMinMaxClosingVo = new StocksMinMaxClosingVo();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, StocksMinMaxClosingVo>.Context context)
                throws IOException, InterruptedException
        {
            stockData = value.toString().split(",");
            ticker = stockData[0];

            if (StringUtils.equals(ticker, "Ticker"))
            {
                // hack to skip header. bad: this if condition will be evaluated
                // for each input line.
                return;
            }

            stockClosing = Double.valueOf(stockData[5]);

            company.set(ticker);
            stocksMinMaxClosingVo.setMinClosing(stockClosing);
            stocksMinMaxClosingVo.setMaxClosing(stockClosing);

            context.write(company, stocksMinMaxClosingVo);

        }

    }

    public static class StocksMinMaxClosingReducer extends
            Reducer<Text, StocksMinMaxClosingVo, Text, StocksMinMaxClosingVo>
    {
        private StocksMinMaxClosingVo result = new StocksMinMaxClosingVo();

        @Override
        protected void reduce(Text key, Iterable<StocksMinMaxClosingVo> values,
                Reducer<Text, StocksMinMaxClosingVo, Text, StocksMinMaxClosingVo>.Context context) throws IOException,
                InterruptedException
        {
            // reset for each new ticker.
            result.setMinClosing(null);
            result.setMaxClosing(null);

            for (StocksMinMaxClosingVo value : values)
            {
                if (result.getMinClosing() == null || value.getMinClosing() < result.getMinClosing())
                {
                    result.setMinClosing(value.getMinClosing());
                }

                if (result.getMaxClosing() == null || value.getMaxClosing() > result.getMaxClosing())
                {
                    result.setMaxClosing(value.getMaxClosing());
                }

            }

            context.write(key, result);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "stocks_min_max");

        job.setJarByClass(StocksMinMaxClosing.class);
        job.setMapperClass(StocksMinMaxClosingMapper.class);
        job.setReducerClass(StocksMinMaxClosingReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(StocksMinMaxClosingVo.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(StocksMinMaxClosingVo.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

}
