package SalesSameDate;
import java.io.IOException;
import java.util.ArrayList;

import mappers.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SalesSameDateReducer extends Reducer<IntWritable, Tuple, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {	
		ArrayList<String> dates = new ArrayList<String>();
		String customerdates = "";
		
		for(Tuple value: values){
			if(value.getType().equals("ReceiptsToCustomers")){
				dates.add(value.getContent().split(",")[1]);
			}else if(value.getType().equals("Customers")){
				String name = value.getContent().split(",")[1];
				String lastname = value.getContent().split(",")[0];
				customerdates = name+","+lastname;
			}
		}
		
		customerdates += ",[";
		for(String date: dates){
			customerdates += date+",";
		}
		customerdates = customerdates.substring(0,customerdates.length()-1);
		customerdates += "]";
		
		context.write(key, new Text(customerdates));
	}
}