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

public class TokenizerMapper1Job2
     extends Mapper<Object, Text, Text, IntWritable>{

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
	int total =0;
	String content;
	String[] str = new String[2];
	
    StringTokenizer itr = new StringTokenizer(value.toString());
    
    	content = value.toString();
    	System.out.println("The content is: " + content);
    	str = content.split("\t");
    	System.out.println("The len is "+str.length);
    	System.out.println("The str is "+str[0]);
    	
    	
    	total++;
    	word.set(str[0]);
    	one.set(Integer.parseInt(str[1]));
    	
    	
    	//System.out.println("The word is:" + str[0] +"The count is: "+str[1]);
    	context.write(word,one);
    	
    	System.out.println("The total is: " + total);
    	Text totalKey = new Text(" ");
    	IntWritable totalWriteable = new IntWritable(total);
    	context.write(totalKey, totalWriteable);
  }
}