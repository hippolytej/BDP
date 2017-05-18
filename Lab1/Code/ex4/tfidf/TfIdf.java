package ecp.bigdata.lab1.ex4.tfidf;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TfIdf extends Configured implements Tool {
    
	public int run(String[] args) throws Exception {

        if (args.length != 2) {
            System.out.println("Usage: [input] [output]");
            System.exit(-1);
        }
        
        Path inputPath = new Path(args[0]);
        Path step1 = new Path("temp1");
        Path step2 = new Path("temp2");
        Path outputPath = new Path(args[1]);
        
        // Suppression du fichier de sortie s'il existe déjà
        FileSystem fs = FileSystem.newInstance(getConf());
        FileStatus[] stat = fs.listStatus(inputPath);
        
        
        
        if (fs.exists(step1)) {
        	fs.delete(step1, true);
        }
        if (fs.exists(step2)) {
        	fs.delete(step2, true);
        }
        if (fs.exists(outputPath)) {
        	fs.delete(outputPath, true);
        }
        
        fs.close();

        // Word count
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("Step 1 : simple WordCount");

        job1.setJarByClass(TfIdf.class);
        job1.setMapperClass(WCMapper.class);
        job1.setReducerClass(WCReducer.class);

        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
		job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        FileInputFormat.addInputPath(job1, inputPath);
        FileOutputFormat.setOutputPath(job1, step1);
        
        if (!job1.waitForCompletion(true)) {
        	System.exit(1);
        }

        // Word per doc
        Job job2 = Job.getInstance(getConf());
        job2.setJobName("Step 2 : WordCount + wordperdoc");

        job2.setJarByClass(TfIdf.class);
        job2.setMapperClass(WCPerDocMapper.class);
        job2.setReducerClass(WCPerDocReducer.class);

        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(Text.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(Text.class);
		job2.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        FileInputFormat.addInputPath(job2, step1);
        FileOutputFormat.setOutputPath(job2, step2);        
        
        if (!job2.waitForCompletion(true)) {
        	System.exit(1);
        }
        
     // docs per word
        Job job3 = Job.getInstance(getConf());
        job3.setJobName("Step 3 : docs per word + tfidf");

        job3.setJarByClass(TfIdf.class);
        job3.setMapperClass(DCMapper.class);
        job3.setReducerClass(DCReducer.class);

        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(Text.class);
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(DoubleWritable.class);
		job3.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");
		job3.setJobName(String.valueOf(stat.length));

        FileInputFormat.addInputPath(job3, step2);
        FileOutputFormat.setOutputPath(job3, outputPath);    
        
        
        return job3.waitForCompletion(true) ? 0: 1;
    }

    public static void main(String[] args) throws Exception {
        TfIdf TfIdfDriver = new TfIdf();
        int res = ToolRunner.run(TfIdfDriver, args);
        
        System.exit(res);
    }
}
