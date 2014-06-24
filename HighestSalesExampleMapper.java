import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class HighestSalesExampleMapper 
  extends Mapper<LongWritable, Text, Text, FloatWritable> {

  
  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    String[] line = value.toString().split("\t");
    
    	  context.write(new  Text(line[2]), new FloatWritable(Float.parseFloat(line[4])));
  }

}

    
