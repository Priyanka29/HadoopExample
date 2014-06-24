/*
	Arthur: Priyanka29
	Descritpion: MapReduce Program to find the sum of sales for each weekday.
*/
import java.io.IOException;
import javax.swing.text.html.HTMLDocument.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SumSalesWeekdayReducer
  extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
	  int sum=0;
	  for(FloatWritable value:values)
	  {
		  sum+=value.get();
		  
	  }
	  context.write(key, new FloatWritable(sum));
  }

}

