/*
	Arthur: Priyanka29
	Descritpion: MapReduce Program to find the sum of sales for each weekday.
*/
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;



public class SumSalesWeekdayMapper 
  extends Mapper<LongWritable, Text, Text, FloatWritable> {

  @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  String[] list = value.toString().split("\t");
	 SimpleDateFormat format = new SimpleDateFormat("yyyy-mm-dd");
	 DateFormat format1= new SimpleDateFormat("EEE");
	 try {
		Date d = format.parse(list[0]);
		String day = format1.format(d);
			context.write(new Text(day), new FloatWritable(Float.parseFloat(list[4])));

	} catch (ParseException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
		
  }

}

    
