package com.badal.test;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PalindromeMapReduce {
	
	public static class PalindromeMapper extends Mapper<Object, Text, Text, Text>
	{
		public static boolean isPalindrome(String str) {
			if (str == null || str.length() <=1)
			{
				return false;
			}
			
		    return str.equals(new StringBuilder(str).reverse().toString());
		}
		
		public static final Text YES = new Text("yes");

		@Override
		protected void map(Object key, Text value,
				Mapper<Object, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			
			boolean isPalindrome = isPalindrome(value.toString());
			
			if (isPalindrome)
			{
				context.write(value, YES);
			}
			
		}
		
		
		
	}
	
	public static class PalindromeReducer extends Reducer<Text, Text, Text, Text>
	{

		@Override
		protected void reduce(Text key, Iterable<Text> values,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			//System.out.println(key);
			//context.write(key, NullWritable.get());
			for (Text value : values)
			{
				context.write(key, value);
				//context.write(key, NullWritable.get());
			}
			
			
		}
		
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "palindrome_example");
		job.setJarByClass(PalindromeMapReduce.class);
		job.setMapperClass(PalindromeMapper.class);
		job.setCombinerClass(PalindromeReducer.class);
		job.setReducerClass(PalindromeReducer.class);
		job.setOutputKeyClass(Text.class);
		//job.setOutputValueClass(Text.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true)? 0 : 1);
		
		
		
		
		
		
		
	}

}
