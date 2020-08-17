//student ID: 497666
//Student Name: Fengmei Liu

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class IntSumReducer1
     extends Reducer<Text,IntWritable,Text,IntWritable> {
  private IntWritable result = new IntWritable();
  private IntWritable total = new IntWritable();
  

  public void reduce(Text key, Iterable<IntWritable> values,
                     Context context
                     ) throws IOException, InterruptedException {
	  
	 
	int sum = 0;
    for (IntWritable val : values) {
      sum += val.get();
      }
    
    result.set(sum);
//    
//    if(key.toString().equals("*"))
//    {
//    	Text total = new Text(" ");
//    	context.write(total, result);
//    }
//    else
//    {
//    	 context.write(key, result);
//    }
    
    context.write(key, result);
   }
}