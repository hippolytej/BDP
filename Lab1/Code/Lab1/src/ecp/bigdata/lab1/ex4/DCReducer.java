package ecp.bigdata.lab1.ex4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DCReducer extends Reducer<Text, Text, Text, DoubleWritable> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String word = key.toString();
		Set<String> docs = new HashSet<String>();
		
		for (Text val : values) {
			String newDoc = val.toString().substring(0, val.toString().indexOf(';') + 1);
			docs.add(newDoc);
		}
		
		for (Text val : values) {
			String[] dwpd = val.toString().split(";");
			
			String wd = word+";"+dwpd[0];
			double wc = Integer.parseInt(dwpd[1]);
			double wpd = Integer.parseInt(dwpd[2]);
			double dpw = docs.size();
			double tfidf = (wc/wpd)*Math.log(2/dpw);
			
			context.write(new Text(wd), new DoubleWritable(tfidf));
		}
	}
}