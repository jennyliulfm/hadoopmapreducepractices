//student ID: 497666
//Student Name: Fengmei Liu

import java.io.IOException;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WeatherReducer1 extends Reducer<Text, Text, Text, DoubleWritable> {

	// This is so that we can write to multiple output files from the same reducer task 
    private MultipleOutputs mos;
	
    public void setup(Context context) {
    	mos = new MultipleOutputs(context);
    }

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		// always good to print some stuff
		System.out.println("In Reducer ----------");
		System.out.println("key: " + key.toString());
		
		double sum =0.0;
		int count = 0;
		double avg = 0.0;
		for(Text txt : values)
		{
			sum +=  Double.parseDouble(txt.toString());
			count ++;
		}
		
		//calculate average
		avg = sum /count;
		DoubleWritable result = new DoubleWritable(avg);
		
		//WRITE ALL THE RESULT INTO A FILE INSTEAD OF MUTIPLE FILES ONE BY ONE.
		//mos.write(key, result, key.toString()); 
		context.write(key, result);
	}
	
    public void cleanup(Context context) throws IOException, InterruptedException {
    	mos.close();
 	}
}