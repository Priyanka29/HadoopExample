import java.io.IOException;
import javax.swing.text.html.HTMLDocument.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MeanExampleReducer
  extends Reducer<Text, FloatWritable, Text, FloatWritable> {
  
  @Override
  public void reduce(Text key, Iterable<FloatWritable> values,Context context) throws IOException, InterruptedException {
	  int sum=0;
	  int count =0;
	  for(FloatWritable value:values)
	  {
		  sum+=value.get();
		  count++;
	  }
	  context.write(key, new FloatWritable(sum/count));
  }

}

