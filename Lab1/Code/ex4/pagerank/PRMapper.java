package ecp.bigdata.lab1.ex4.pagerank;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

//import ecp.bigdata.lab1.ex4.pagerank.PageRank.CustomCounters;

import java.io.IOException;
import java.util.StringTokenizer;

public class PRMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] in_outList = value.toString().split(";");
		int currentNode = Integer.parseInt(in_outList[0]);
		context.write(new IntWritable(currentNode), new Text("#" + in_outList[1]));
		
		int numConnections = in_outList[1].split(",").length;
//		long numNodes = context.getCounter(CustomCounters.NODECOUNTER).getValue();
		double pagerank = ( (double) 1 / (double) 75879) / numConnections ;
		
		StringTokenizer tokenizer = new StringTokenizer(in_outList[1], ",");
		while (tokenizer.hasMoreTokens()) {
			int linkedNode = Integer.parseInt(tokenizer.nextToken());
			context.write(new IntWritable(linkedNode), new Text(String.valueOf(pagerank)));
			
		}
	}
}