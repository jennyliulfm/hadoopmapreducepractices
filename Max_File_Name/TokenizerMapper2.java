//student ID: 497666
//Student Name: Fengmei Liu

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.util.LinkedHashMap;
import java.util.Map;

public class TokenizerMapper2
     extends Mapper<Object, Text, Text, Text>{

  private final static IntWritable one = new IntWritable(1);
  private Text word = new Text();
  
  private Text fileName = new Text();
  private Text value = new Text();
  private Text output  = new Text("*");
 
  
  public void map(Object key, Text value, Context context
                  ) throws IOException, InterruptedException {
	
	FileSplit filesplit = (FileSplit)context.getInputSplit();
  	Path filepath = filesplit.getPath();
  	String filename = filesplit.getPath().getName();
  	FileSystem fs = FileSystem.get(new Configuration());
  	BufferedReader  br = new BufferedReader(new InputStreamReader(fs.open(filepath)));
  	String content="";
  	String line = br.readLine();
  	String strtmp;
  	while(line!=null)
  	{
  		content = content+ " " +line;
  		line = br.readLine();
  	}
  	System.out.println("The file name " +filename + " The conent" + content);
  	
  	String[] strArray = content.split(" ");
  	Map<String, String> hMap = new LinkedHashMap<String, String>();
  	for(int i =1; i<strArray.length; i++) {
  		System.out.println("The split content is ");
  		System.out.println(strArray[i]);
  		int wordcount = 1;
  		if(!hMap.containsKey(strArray[i]))
  		{
  			hMap.put(strArray[i], "1");
  		}
  		}
  	
  	System.out.println("The hash map is:");
  	System.out.println(hMap);
  	
  	int count =0;
  	count= hMap.size();
  	one.set(count);
  	word.set(filename);
  	strtmp = filename +" "+count;
  	value.set(strtmp);
  	context.write(output, value);
  	}
}
