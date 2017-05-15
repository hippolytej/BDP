package ecp.bigdata.lab1.ex4.pagerank;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;
import ecp.bigdata.lab1.ex4.pagerank.PageRank.CustomCounters;
import java.io.IOException;

public class PreReducer extends Reducer<IntWritable, IntWritable, IntWritable, Text> {
	@Override
	public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		
		String outList = new String();
		
		for (IntWritable val : values){
			outList = outList + "," + val.toString();
		}
		
		outList = outList.substring(1);
		
		context.write(key, new Text(outList));
		
		context.getCounter(CustomCounters.NODECOUNTER).increment(1);
	}
}