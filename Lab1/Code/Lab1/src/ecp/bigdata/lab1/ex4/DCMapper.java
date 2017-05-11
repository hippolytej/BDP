package ecp.bigdata.lab1.ex4;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class DCMapper extends Mapper<Text, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] wdcp = value.toString().split(";");
		context.write(new Text(wdcp[0]), new Text(wdcp[1]+";"+wdcp[2]+";"+wdcp[3]));
	}
}