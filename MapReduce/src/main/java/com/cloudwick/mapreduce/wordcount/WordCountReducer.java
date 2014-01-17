package com.cloudwick.mapreduce.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WordCountReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
	protected void reduce(Text word, Iterable<IntWritable> values, Context context)
			throws java.io.IOException, InterruptedException {
		int count = 0;
		
		for (IntWritable value : values) {
		 count += value.get();
		}
		
		context.write(word, new IntWritable(count));
	};

}
