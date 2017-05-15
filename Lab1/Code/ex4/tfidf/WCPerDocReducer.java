package ecp.bigdata.lab1.ex4.tfidf;

import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class WCPerDocReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		
		String doc = key.toString();
		Map<String, String> wordCountMap = new HashMap<String, String>();
		int wordsPerDoc = 0;
		
		for (Text val : values) {
			String[] wc = val.toString().split(";");
			wordCountMap.put(wc[0], wc[1]);
			wordsPerDoc += Integer.parseInt(wc[1]);
		}
		
		Set<Entry<String, String>> setMap = wordCountMap.entrySet();
		Iterator<Entry<String, String>> it = setMap.iterator();
		
		while(it.hasNext()){
			
			Entry<String, String> e = it.next();
			String wd = e.getKey()+";"+doc;
			String wcWpd = e.getValue()+";"+wordsPerDoc;
			
			context.write(new Text(wd), new Text(wcWpd));
		}
	}
}