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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.*;

public class IntSumReducer2
     extends Reducer<Text,Text,Text,IntWritable> {
  private IntWritable result = new IntWritable();
  private Text filename = new Text();
  

  public void reduce(Text key, Iterable<Text> values,
                     Context context
                     ) throws IOException, InterruptedException {
	  
	Map<String, String> hMap = new LinkedHashMap<String, String>();
	int sum = 0;
	String[] str = new String[3];
	for (Text val : values) {
    	str = val.toString().split(" ");
    	System.out.println("the file name is:" +str[0]);
    	System.out.println("the size  is:" +str[1]);
    
    	hMap.put(str[0], str[1]);
    	}
	
	Iterator<Map.Entry<String, String>> it = hMap.entrySet().iterator();
	Map.Entry<String, String> pair;
	int max = 0;
	String maxFileName ="";
	
	if(it.hasNext())
	{
		pair = it.next();
		max = Integer.parseInt(pair.getValue());
		maxFileName = pair.getKey();
	}
	
	while(it.hasNext())
	{
		pair = it.next();
		int maxTmp = Integer.parseInt(pair.getValue());
		String filename = pair.getKey();
		
		if(maxTmp > max)
		{
			max = maxTmp;
			maxFileName = filename;
		}
	}
	
	//filename.set("Testing");
	filename.set(maxFileName);
	result.set(max);
    
   // result.set(sum);
    context.write(filename, result);
    }
}