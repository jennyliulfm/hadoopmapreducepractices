//student ID: 497666
//Student Name: Fengmei Liu

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.join.TupleWritable;
import org.apache.hadoop.fs.Path;

public class WeatherJob1 {

	public static void main(String[] args) throws Exception {
		
		String input = args[0];
		String output = args[1];

	    Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Weather Job1");
		
		job.setJarByClass(WeatherJob1.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class); 
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job, new Path(input));
		FileOutputFormat.setOutputPath(job, new Path(output));

		// TextInputFormat will make sure that we are fed the text file line by line
		job.setInputFormatClass(TextInputFormat.class); 
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapperClass(WeatherMapper1.class);
		job.setReducerClass(WeatherReducer1.class);
				
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}

}