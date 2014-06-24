/*
	Author: Priyanka29
	Descritpion: MapReduce Program to find the sum of sales for each weekday.
*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class SumSalesWeekday extends Configured implements Tool{
		
	
	public int run(String[] args) throws Exception{
		Job job = new Job();
	    job.setJarByClass(SumSalesWeekday.class);
	    job.setJobName("SumSalesWeekday");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(SumSalesWeekdayMapper.class);
	    job.setCombinerClass(SumSalesWeekdayReducer.class);
	    job.setReducerClass(SumSalesWeekdayReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new SumSalesWeekday(), args);
	  System.exit(exitCode);
    
   }
}
