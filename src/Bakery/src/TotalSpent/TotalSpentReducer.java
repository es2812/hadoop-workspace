package TotalSpent;
import java.io.IOException;

import mappers.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalSpentReducer extends Reducer<IntWritable, Tuple, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
		
		String customer = "";
		double finalPrice = 0.0;
		for(Tuple value: values){
			if(value.getType().equals("Customers")){
				customer = value.getContent();
				String name = customer.split(",")[1];
				String lastname = customer.split(",")[0];
				customer = name+","+lastname;
			}else if(value.getType().equals("PricedReceiptsToCustomers")){
				finalPrice += Double.parseDouble(value.getContent());
			}
		}
		context.write(key,new Text(customer+","+String.valueOf(finalPrice)));
	}
}