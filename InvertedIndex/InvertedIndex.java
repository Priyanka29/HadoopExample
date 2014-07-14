/*
  Author: Priyanka29
  Description: Find the inverted index for a particular word from forum data.
*/

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class InvertedIndex extends Configured implements Tool{
		
	
	@Override
	public int run(String[] args) throws Exception{
		Job job = new Job();
	    job.setJarByClass(InvertedIndex.class);
	    job.setJobName("InvertedIndex");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(InvertedIndexMapper.class);
	    //job.setMapOutputKeyClass(Text.class);
	    //job.setMapOutputValueClass(Text.class);
	    //job.setCombinerClass(InvertedIndexReducer.class);
	    job.setReducerClass(InvertedIndexReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new InvertedIndex(), args);
	  System.exit(exitCode);
    
   }
}
