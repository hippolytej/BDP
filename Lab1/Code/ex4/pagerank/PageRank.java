package ecp.bigdata.lab1.ex4.pagerank;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class PageRank extends Configured implements Tool {
    
	public static enum CustomCounters{NODECOUNTER}
	
	public int run(String[] args) throws Exception {

        if (args.length != 3) {
            System.out.println("Usage: [input] [output] [nb iter]");
            System.exit(-1);
        }
        
        Path inputPath = new Path(args[0]);
        Path step = new Path("temp");
        Path iterIn = new Path("iterIn");
        Path iterOut = new Path("iterOut");
        Path outputPath = new Path(args[1]);
        
        FileSystem fs = FileSystem.newInstance(getConf());
        if (fs.exists(step)) {
        	fs.delete(step, true);
        }
        if (fs.exists(iterIn)) {
        	fs.delete(iterIn, true);
        }
        if (fs.exists(iterOut)) {
        	fs.delete(iterOut, true);
        }
        if (fs.exists(outputPath)) {
        	fs.delete(outputPath, true);
        }
        
        Job job0 = Job.getInstance(getConf());
        job0.setJobName("Step 0 : process input");

        job0.setJarByClass(PageRank.class);
        job0.setMapperClass(PreMapper.class);
        job0.setReducerClass(PreReducer.class);

        job0.setMapOutputKeyClass(IntWritable.class);
        job0.setMapOutputValueClass(IntWritable.class);
        job0.setOutputKeyClass(IntWritable.class);
        job0.setOutputValueClass(Text.class);
		job0.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        FileInputFormat.addInputPath(job0, inputPath);
        FileOutputFormat.setOutputPath(job0, step);
        
        if (!job0.waitForCompletion(true)) {
        	System.exit(1);
        }
        
        Counter counter = job0.getCounters().findCounter(CustomCounters.NODECOUNTER);
        System.out.println("Nombre de nodes :" + counter.getValue());
        
        Job job1 = Job.getInstance(getConf());
        job1.setJobName("Initialize PageRank");

        job1.setJarByClass(PageRank.class);
        job1.setMapperClass(PRMapper.class);
        job1.setReducerClass(PRReducer.class);

        job1.setMapOutputKeyClass(IntWritable.class);
        job1.setMapOutputValueClass(Text.class);
        job1.setOutputKeyClass(IntWritable.class);
        job1.setOutputValueClass(Text.class);
		job1.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

        FileInputFormat.addInputPath(job1, step);
        FileOutputFormat.setOutputPath(job1, iterIn);    
        
        if (!job1.waitForCompletion(true)) {
        	System.exit(1);
        }
        
        int nIter = Integer.parseInt(args[2]);
        int i = 1;
        while(true){
        	
        	System.out.println("__________________________________________");
        	System.out.println("              ITERATION #" + i);
        	System.out.println("__________________________________________");
        	
        	Job job = Job.getInstance(getConf());
            job.setJobName("Step  : iterate pagerank");

            job.setJarByClass(PageRank.class);
            job.setMapperClass(SimpleMapper.class);
            
            job.setMapOutputKeyClass(IntWritable.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(IntWritable.class);
            job.setOutputValueClass(Text.class);
    		job.getConfiguration().set("mapreduce.output.textoutputformat.separator", ";");

            FileInputFormat.addInputPath(job, iterIn);
            
            if (i == nIter){
            	job.setReducerClass(FinalReducer.class);
            	FileOutputFormat.setOutputPath(job, outputPath);
            	fs.close();
            	return job.waitForCompletion(true) ? 0: 1;
            }
              
            else {
            	job.setReducerClass(PRReducer.class);
            	FileOutputFormat.setOutputPath(job, iterOut); 
            	if (!job.waitForCompletion(true)) {
                	System.exit(1);
                }
            	fs.delete(iterIn, true);
            	FileUtil.copy(fs, iterOut, fs, iterIn, true, getConf());
            	fs.delete(iterOut, true);
            }
            
        	i++;
        }
    }

    public static void main(String[] args) throws Exception {
        PageRank PRDriver = new PageRank();
        int res = ToolRunner.run(PRDriver, args);
        System.exit(res);
    }
}
