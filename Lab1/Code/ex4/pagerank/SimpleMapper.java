package ecp.bigdata.lab1.ex4.pagerank;

import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class SimpleMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
	
	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {

		String line = value.toString();


		String[] cle_val = line.split(";");
		String val = new String();

		int cle = Integer.parseInt(cle_val[0]);
		if (cle_val.length == 2){
			val = cle_val[1];
		}
		else{
			val = "";
		}

		context.write(new IntWritable(cle), new Text(val));
	}
}
