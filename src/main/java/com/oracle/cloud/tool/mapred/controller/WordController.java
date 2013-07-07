package com.oracle.cloud.tool.mapred.controller;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.hadoop.fs.FsShell;
import org.springframework.stereotype.Controller;
import org.springframework.util.Assert;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import com.oracle.cloud.tool.mapred.WordCount.IntSumReducer;
import com.oracle.cloud.tool.mapred.WordCount.TokenizerMapper;

@Controller
public class WordController {

	@Autowired
	private Job job;
	@Autowired
	private Configuration configurable;
	
	@RequestMapping(value="/run", method=RequestMethod.GET)
	@ResponseBody
	public String runMapRed() throws Exception{
		Assert.notNull(job);
		FsShell fs = new FsShell(job.getConfiguration());
		if(fs.test("/user/sharadgaur/Out")) {
			fs.rmr("/user/sharadgaur/Out");
		}
		fs.close();
		return job.waitForCompletion(true) + "";
	}

	@RequestMapping(value="/runCustom", method=RequestMethod.GET)
	@ResponseBody
	public String runCustomMapRed() throws Exception{
		Assert.notNull(job);
		FsShell fs = new FsShell(job.getConfiguration());
		if(fs.test("/user/sharadgaur/Out")) {
			fs.rmr("/user/sharadgaur/Out");
		}
		fs.close();
	
		Job job = new  Job(configurable);
		job.setMapperClass(TokenizerMapper.class);
		job.setReducerClass(IntSumReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		org.apache.hadoop.mapreduce.lib.input.FileInputFormat.addInputPath(job, new Path("/user/sharadgaur/In/data"));
		FileOutputFormat.setOutputPath(job, new Path("/user/sharadgaur/Out"));
		
		return job.waitForCompletion(true) + "";
	}

}
