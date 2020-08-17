//student ID: 497666
//Student Name: Fengmei Liu

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

public class WeatherReducer2 extends Reducer<Text, Text, Text, DoubleWritable> {
	
	//Hash Map is to save the data for the 
	HashMap<String, Double> hMap = new HashMap<String, Double>();
	List<String> stationName = new LinkedList<String>();

	// This makes it possible to write to multiple output files
    private MultipleOutputs mos;
    public void setup(Context context) {
    	mos = new MultipleOutputs(context);
    }

	// WRITE YOUR CODE HERE
	// HINT: take a look at the wordcount example for inspiration on how to count 
	// items in the reducer (located at `/home/hduser/wordcount/IntSumReducer.java`)
	// ...
	
	
	
	
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		//To get the stationID, year and average temperature, then save them into a map.
		String content;
		String[] str = new String[3];
		for(Text txt: values)
		{
			String mapKey="";
			Double mapValue =0.0;
			content = txt.toString();
			str = content.split("\t");
			System.out.println("The content is: " +content);
			for(int i =0; i<str.length;i++)
			{
				System.out.println("The value in the str is: " +str[i]+" ");
				
			}
		//System.out.println("The station is :" +str[0] + " the year is: " + str[1] + " The average is:" +str[2]);
			
			mapKey = str[0]+"\t"+str[1];
			mapValue = Double.parseDouble(str[2]);
			hMap.put(mapKey, mapValue);
			stationName.add(str[0]);
			
			//context.write(key, Double.parseDouble(content));
		}
		
		//To get the total number of the year
		//As the stationName and year totally match with each other, 
		//so the total count of the station name in the list of stationname
		//are the number of the record's year for that station. 
		String sNameTmp ="";
		
		int totalYear1 =0;
		String statinID1="";
		
		int totalYear2 =0;
		String stationID2="";
		
		int totalYear =0;
		String maxStationID ="";
		
		if(stationName.size()>0)
		{
			sNameTmp = stationName.get(0);
		}
		
		for(int i=0; i<stationName.size(); i++)
		{
			String name = stationName.get(i);
			System.out.println("The station name" +i +" is "+name);
			
			if(name.equals(sNameTmp))
			{
				totalYear1++;
				statinID1 =name;
				
			}
			else
			{
				totalYear2++;
				stationID2 = name;
			}
		}
		
		//As to get the span of the year. 
		if(totalYear1>totalYear2)
		{
			
			totalYear = totalYear1;
			maxStationID = statinID1;
		}
		else
		{
			
			totalYear = totalYear2;
			maxStationID = stationID2;
			
		}
		
		System.out.println("The total year is: "+totalYear);
		System.out.println("The Max statioin is: "+maxStationID);
		System.out.println("The year of station1 is " +totalYear1);
		System.out.println("The year of station2 is " +totalYear2);
		
		
		//get the average for each year and do the calculation
		Iterator<Map.Entry<String, Double>> it = hMap.entrySet().iterator();
		
		Map.Entry<String, Double> pair;
		double avg1 =0.0;
		double avg2 =0.0;
		
		double sum = 0.0;
		
		int count= 0;
		while(it.hasNext())
		{
			pair = it.next();
			String tmp ="";
			tmp = pair.getKey();
			
			String[] stationYear =new String[3];
			stationYear = tmp.split("\t");
			
			String stationtmp = stationYear[0];
			int year = Integer.parseInt(stationYear[1]);
			
			if(stationtmp.equals(maxStationID))
			{
				avg1 = pair.getValue();
				avg2 = getAverageByYear(maxStationID,year);
				double diff = Math.abs(avg1 - avg2);
				System.out.print("the year is " + year +" ");
				System.out.print("The avg1 is: " +avg1 + " ");
				System.out.print("The avg2 is: " + avg2 + " ");
				System.out.println("The diff is: " + diff + " ");
				
				sum += diff;
				count++;
				if(count == totalYear)
				{
					break;
				}
			}
		}
		
		double similarity = sum / totalYear;
		
		System.out.println("The sum of average is: " + sum);
		//mos.write(key, value, baseOutputPath);
		
		Text outputKey = new Text();
		outputKey.set(statinID1 + "\t" + stationID2 );
		
		DoubleWritable outputValue = new DoubleWritable();
		outputValue.set(similarity);
		context.write(outputKey, outputValue);		
}
	
	public double getAverageByYear(String stationID, int year) {
		
		double avg = 0.0;
		
		Iterator<Map.Entry<String, Double>> it = hMap.entrySet().iterator();
	
		Map.Entry<String, Double> pair;
		String stationID2 ="";
		int year2 =0;
		String[] stationYear =new String[3];
		String tmp ="";
		
		while(it.hasNext())
		{
			pair = it.next();
			tmp = pair.getKey();
			stationYear = tmp.split("\t");
			
			stationID2 = stationYear[0];
			year2 = Integer.parseInt(stationYear[1]);
			
			if((year2==year) && (!stationID2.equals(stationID)))
			{
				avg = pair.getValue();
				break;
			}
			
		}
		
		
		return avg;
		
	}
	
	// just to clean up some stuff..
    public void cleanup(Context context) throws IOException, InterruptedException {
    	mos.close();
 	}
}