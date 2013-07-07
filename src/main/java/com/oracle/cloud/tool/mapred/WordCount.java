package com.oracle.cloud.tool.mapred;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.springframework.context.support.AbstractApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.data.hadoop.fs.FsShell;

public class WordCount {
	private static final Log log = LogFactory.getLog(WordCount.class);

	public static void main(String[] args) throws Exception {
		
		AbstractApplicationContext context = new ClassPathXmlApplicationContext(
				"hadoop-context.xml");
		
		context.registerShutdownHook();
	}
	
	public static class IntSumReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context)
				throws IOException, InterruptedException {
		
			int count=0;
			for(LongWritable value: values) {
				count++;
			}
			context.write(key, new LongWritable(count));
		}
	}
	
	
	public static class TokenizerMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
		private LongWritable one = new LongWritable(1l);
		private Text word = new Text();
		@Override
		protected void map(LongWritable key, Text value,
				org.apache.hadoop.mapreduce.Mapper.Context context)
				throws IOException, InterruptedException {
			
			StringTokenizer st = new StringTokenizer(value.toString());
			while(st.hasMoreTokens()) {
				word.set(st.nextToken());
				context.write(word,one);
			}
			
		}
	}

}
