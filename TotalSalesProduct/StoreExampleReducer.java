/*
  Author: Priyanka29
  Description: Find the total sales of a particular product.
*/
import java.io.IOException;
import java.text.DecimalFormat;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class StoreExampleReducer
  extends Reducer<Text, FloatWritable, Text, Text> {
	  DecimalFormat df = new DecimalFormat("0.##");
	    
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values,
      Context context)
      throws IOException, InterruptedException {
	  df.setMaximumFractionDigits(2);
	  float salesTotal = 0;
	  for (FloatWritable value : values) {
        salesTotal += value.get();
      }
    context.write(key, new Text(df.format(salesTotal)));
  }

}

