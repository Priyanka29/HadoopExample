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

public class MeanExample extends Configured implements Tool{
		
	
	public int run(String[] args) throws Exception{
		Job job = new Job();
	    job.setJarByClass(MeanExample.class);
	    job.setJobName("MeanExample");

	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    job.setMapperClass(MeanExampleMapper.class);
	    job.setReducerClass(MeanExampleReducer.class);

	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(FloatWritable.class);
	    
	    return job.waitForCompletion(true) ? 0 : 1;
	}
	
  public static void main(String[] args) throws Exception {
	  int exitCode = ToolRunner.run(new MeanExample(), args);
	  System.exit(exitCode);
    
   }
}
