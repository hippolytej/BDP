package ecp.bigdata.lab1.ex4;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WCPerDocReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String doc = key.toString();
		int wordsPerDoc = 0;
		for (Text val : values) {
			String count = val.toString().substring(val.toString().lastIndexOf(';') + 1);
			wordsPerDoc += Integer.parseInt(count);
		}
		
		for (Text val : values) {
			String[] wc = val.toString().split(";");
			
			String wd = wc[0]+";"+doc;
			String wcWpd = wc[1]+";"+wordsPerDoc;
			
			context.write(new Text(wd), new Text(wcWpd));
		}
	}
}