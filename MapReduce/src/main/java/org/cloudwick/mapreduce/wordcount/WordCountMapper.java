package org.cloudwick.mapreduce.wordcount;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class WordCountMapper extends
		Mapper<LongWritable, Text, Text, IntWritable> {

	protected void map(LongWritable key, Text value, Context context)
			throws java.io.IOException, InterruptedException {
		
		String line = value.toString();
		String[] arr = line.split(",");
		for (int i = 0; i < arr.length; i++) {
			context.write(new Text(arr[i]), new IntWritable(1));
		}
		 
	};

}
