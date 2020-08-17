
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

public class TokenizerMapper1
     extends Mapper<Object, Text, Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  private Text spKey = new Text("*");
  int len =0;
  
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
	  String str = value.toString();
	  str = str.replaceAll("\\W", " "); 
	  System.out.println("The content is: ");
	  System.out.println(str);
	  StringTokenizer itr = new StringTokenizer(str);
    while (itr.hasMoreTokens()) {
      word.set(itr.nextToken());
      
      len = word.getLength();
      
      if(len > 4)
      {
    	  if(word.toString().matches("^[a-zA-Z]*$"))
    	  {
    		  context.write(word, one);
    		  //context.write(spKey, one);
    	  }
      }
    }
  }
 }