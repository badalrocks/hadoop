package com.badal.mapreduce.samples;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * This is an example of Map Reduce that uses Bloom filter algorithm. bloom_input.txt contains the ticker symbols that
 * should be filtered in. creates bloom_output file that stores the bytes from bloom_input.txt file. Map phase uses
 * bloom_output to test whether a particular record should be filtered in or out. Reduce phase works business as usual.
 * 
 * @author badal
 *
 */
public class BloomFilterMapReduce
{
    public static class BloomFilterMapReduceMapper extends Mapper<Object, Text, Text, DoubleWritable>
    {
        private BloomFilter bloomFilter;

        /**
         * Uses bloom output file to test membership of each record. If membership test fails, then discards that
         * record.
         */
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

            if (!bloomFilter.membershipTest(new Key(ticker.getBytes())))
            {
                return;
            }

            context.write(new Text(ticker), new DoubleWritable(Double.valueOf(stockData[5])));

        }

        @Override
        protected void setup(Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            setupBloomFilter(context);
        }

        /**
         * Reads bloom input file that contains all keys to be filtered in. Add those keys to bloom output file.
         * 
         * @param context
         * @throws IOException
         * @throws FileNotFoundException
         */
        private void setupBloomFilter(Mapper<Object, Text, Text, DoubleWritable>.Context context) throws IOException,
                FileNotFoundException
        {
            bloomFilter = new BloomFilter(10, 2, Hash.MURMUR_HASH);

            FileSystem fs = FileSystem.get(context.getConfiguration());

            File bloomInputFile = new File(context.getConfiguration().get("bloom.input.file"));

            BufferedReader br = null;
            FSDataOutputStream fsdos = null;
            try
            {
                br = new BufferedReader(new FileReader(bloomInputFile));

                String line = null;
                while ((line = br.readLine()) != null)
                {
                    bloomFilter.add(new Key(line.getBytes()));
                }

                fsdos = fs.create(new Path(context.getConfiguration().get("bloom.output.file")));
                bloomFilter.write(fsdos);

            }
            finally
            {
                br.close();
                fsdos.flush();
                fsdos.close();
            }
        }

    }

    public static class BloomFilterMapReduceReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable>
    {

        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values,
                Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context) throws IOException,
                InterruptedException
        {
            double totalClosing = 0;
            int totalDays = 0;

            for (DoubleWritable value : values)
            {
                totalClosing += value.get();
                totalDays++;
            }

            double average = totalClosing / totalDays;

            context.write(key, new DoubleWritable(average));

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        conf.set("bloom.input.file", args[0]); // contains keys that should be filtered in.
        conf.set("bloom.output.file", args[1]); // binary file that stores all keys from bloom input file.

        Job job = Job.getInstance(conf, "bloom_filter_map_reduce");

        job.setJarByClass(BloomFilterMapReduce.class);
        job.setMapperClass(BloomFilterMapReduceMapper.class);
        job.setReducerClass(BloomFilterMapReduceReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[2]));
        FileOutputFormat.setOutputPath(job, new Path(args[3]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
