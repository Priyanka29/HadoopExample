/*
  Author: Priyanka29
  Description: Find the total sales of all stores.
*/
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalSalesExampleReducer
  extends Reducer<NullWritable,FloatWritable, LongWritable, Text> {
	private static long totalNumSales= 0;
	private static float salesTotal = 0;
	DecimalFormat fs = new DecimalFormat("#0.##");
  
  @Override
  public void reduce(NullWritable key, Iterable<FloatWritable> values,
      Context context)
      throws IOException, InterruptedException {
	  //fs.setMaximumFractionDigits(2);
	  for (FloatWritable value : values) {
    	 salesTotal += value.get();
    	 totalNumSales++;
        
      }
      context.write(new LongWritable(totalNumSales), new Text(fs.format(salesTotal)));
  }
  

}

