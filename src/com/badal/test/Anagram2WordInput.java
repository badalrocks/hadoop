package com.badal.test;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Anagram2WordInput
{

    public static class Anagram2WordInputMapper extends Mapper<Object, Text, Text, Text>
    {
        public boolean isAnagram(String firstWord, String secondWord)
        {
            char[] word1 = firstWord.replaceAll("[\\s]", "").toCharArray();
            char[] word2 = secondWord.replaceAll("[\\s]", "").toCharArray();
            Arrays.sort(word1);
            Arrays.sort(word2);
            return Arrays.equals(word1, word2);
        }

        public static final Text YES = new Text("anagrams");
        public static final Text NO  = new Text("not anagrams");

        @Override
        protected void map(Object key, Text value, Mapper<Object, Text, Text, Text>.Context context) throws IOException, InterruptedException
        {

            String[] words = value.toString().split(" ");
            boolean isAnagram = isAnagram(words[0], words[1]);

            if (isAnagram)
            {
                context.write(new Text(words[0] + "-" + words[1]), YES);
            }
            else
            {
                context.write(new Text(words[0] + "-" + words[1]), NO);
            }

        }

    }

    public static class Anagram2WordInputReducer extends Reducer<Text, Text, Text, Text>
    {

        @Override
        protected void reduce(Text key, Iterable<Text> values, Reducer<Text, Text, Text, Text>.Context context) throws IOException, InterruptedException
        {

            for (Text value : values)
            {
                context.write(key, value);
            }

        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException
    {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "anagram_2_work_input");
        job.setJarByClass(Anagram2WordInput.class);
        job.setMapperClass(Anagram2WordInputMapper.class);
        job.setCombinerClass(Anagram2WordInputReducer.class);
        job.setReducerClass(Anagram2WordInputReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }

}
