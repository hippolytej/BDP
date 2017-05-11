package ecp.bigdata.lab1.ex4t;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;



public class TestMapper extends Mapper<LongWritable, Text, Text, Text> {


	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String[] wdc = value.toString().split(";");
		context.write(new Text(wdc[1]), new Text(wdc[0]+";"+wdc[2]));
	}
}

