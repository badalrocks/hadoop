package com.badal.mapreduce.samples;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * This program creates an inverted index. It reads all files from input dir and for each word it computes the number of
 * occurrences per word per file. This output can be used in showing search result pages based on keyword search.
 * 
 * @author badal
 *
 */
public class InvertedIndex
{

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>
    {
        Text outKey   = new Text();
        Text outValue = new Text();

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {
            String[] words = value.toString().split(" ");

            // get filename being processed.
            FileSplit fs = (FileSplit) context.getInputSplit();
            String filename = fs.getPath().getName();

            outValue.set(filename);

            for (String word : words)
            {
                outKey.set(word);

                context.write(outKey, outValue);
            }

        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text>
    {
        Text outValue = new Text();

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context)
                throws IOException, InterruptedException
        {

            Map<String, Long> wordCountByFilename = new HashMap<String, Long>();

            for (Text value : values)
            {
                String filename = value.toString();

                if (wordCountByFilename.get(filename) != null)
                {
                    long wordCount = wordCountByFilename.get(filename);
                    wordCountByFilename.put(filename, wordCount + 1);
                }
                else
                {
                    wordCountByFilename.put(filename, 1l);
                }

            }

            outValue.set(wordCountByFilename.toString());

            context.write(key, outValue);

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted_index");

        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.getConfiguration().set("mapreduce.output.basename", "inverted_index");

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
