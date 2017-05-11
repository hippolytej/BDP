package ecp.bigdata.lab1.ex3;


import org.apache.hadoop.io.*;        
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;


public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable ONE = new IntWritable(1);
	private Text word = new Text();

	public void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String line = value.toString().toLowerCase();
		StringTokenizer tokenizer = new StringTokenizer(line, " \t\n\r\f,.:;?![]{}'\"()&<>~_-#$*^%/@\\`=+|");
		while (tokenizer.hasMoreTokens()) {
			word.set(tokenizer.nextToken());
			context.write(word, ONE);
		}
	}
}






