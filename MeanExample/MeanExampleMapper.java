/*
	Author: Priyanka29
	Description: Find the mean of sales for Sunday
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



public class MeanExampleMapper 
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
		if(day.equalsIgnoreCase("sun")){
			context.write(new Text(day), new FloatWritable(Float.parseFloat(list[4])));
		}
	} catch (ParseException e) {
		e.printStackTrace();
	}
		
  }

}

    
