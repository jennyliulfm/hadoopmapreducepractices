//student ID: 497666
//Student Name: Fengmei Liu

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;
import java.util.StringTokenizer;

public class WeatherMapper2 extends Mapper<LongWritable, Text, Text, Text> {
	  private final static IntWritable one = new IntWritable(1);
	  private Text word = new Text();
	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// WRITE YOUR CODE HERE
		//WRITE THE OUTPUT FROM MAPPER-REDUCER1 LINE BY LINE, THEN PASS TO THE REDUCER2
		StringTokenizer itr = new StringTokenizer(value.toString());
		String content = value.toString();
		//String[] strsplit = content.split("\t");
	 
		//Text outputKey = new Text(strsplit[0]);
		//Text outputValue = new Text(strsplit[1]);
		Text outputKey = new Text(" ");
		Text outputValue = new Text();
		outputValue.set(content);
		System.out.println("The key is: " +outputKey + " The value is: " +content);
		//System.out.println("The key is: " + outputKey + "The value is:" +outputValue);
		context.write(outputKey, outputValue);
		}
}