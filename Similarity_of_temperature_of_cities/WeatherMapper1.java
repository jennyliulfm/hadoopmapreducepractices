//student ID: 497666
//Student Name: Fengmei Liu

import java.io.IOException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Mapper;
import java.util.Arrays;

public class WeatherMapper1 extends Mapper<LongWritable, Text, Text, Text> {

	
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		
		// read the input - which is a complete row of the 17xy.csv file
		// split it with "," as delimiter
		String[] fields = value.toString().split(",");
		
		// extract the station-id
		//Text stationId = new Text(fields[0].trim());
		String stationId = fields[0].trim();
		
		// extract the temperature type 
		String tempType = fields[2].trim();
		
		// extract the date. Have to convert it from Long to String
		// you can prob. do better than me :)
		String date = Long.toString(Long.parseLong(fields[1].trim())).substring(0,4);
		String temp = Double.toString(Double.parseDouble(fields[3].trim())/10.0);
		
		//THE OUTPUT KEY IS THE STATIONID AND THE YEAR, THE VALUE IS THE MINIMUM TEMPERATURE OF THAT YEAR
		Text outputKey = new Text();
		outputKey.set(stationId + "\t"+date);

		// We should only collect the min temperature for this mapreduce job
		if(new String("TMIN").equals(tempType)){
			Text v= new Text(temp);
			context.write(outputKey, v);
		}
	}
}