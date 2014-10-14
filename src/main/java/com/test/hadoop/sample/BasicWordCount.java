package com.test.hadoop.sample;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BasicWordCount {

	public static class TokenizerMapper extends
			Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable ONE= new IntWritable(1);
		private final Text WORD = new Text();

		public void map(final Object key, final Text value,
				final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				WORD.set(itr.nextToken());
				context.write(WORD, ONE);
			}
		}
	}

	/**
	 * 
	 * @author lieu
	 * 
	 */
	public static class IntSumReducer extends
			Reducer<Text, IntWritable, Text, IntWritable> {

		private final IntWritable RESULT = new IntWritable();

		public void reduce(final Text key, final Iterable<IntWritable> values,
				final Context context) throws IOException, InterruptedException {
			int sum = 0;
			for (IntWritable val : values) {
				sum += val.get();
			}
			RESULT.set(sum);
			context.write(key, RESULT);
		}
	}

}
