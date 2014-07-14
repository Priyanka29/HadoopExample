/*
  Author: Priyanka29
  Description: Find the total sales of all stores.
*/
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class TotalSalesExampleMapper 
  extends Mapper<LongWritable, Text, NullWritable, FloatWritable> {
	
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] line = value.toString().split("\t");
    
    	  context.write(NullWritable.get(),new FloatWritable(Float.parseFloat(line[4])));
  }

}

    
