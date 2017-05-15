package ecp.bigdata.lab1.ex4.pagerank;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;



public class PreMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String line = value.toString();
		
		if (!line.substring(0, 1).equals("#")){
			String[] in_out = line.split("\t");
			
			int in = Integer.parseInt(in_out[0]);
			int out = Integer.parseInt(in_out[1]);
			
			context.write(new IntWritable(in), new IntWritable(out));
			
		}
	}
}