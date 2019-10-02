package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PricedReceiptsToCustomersMapper extends Mapper<IntWritable, Text, IntWritable, Tuple> {
	
	@Override
	public void map(IntWritable key, Text value, Context context)throws IOException, InterruptedException {
		//FORMAT: key: 		ReceiptID
		//		  value:	Date, CustomerID, Price
		
		String[] fields = value.toString().split(",");
		int customerID = Integer.parseInt(fields[1]);
		String price = fields[2];

		Tuple t = new Tuple("PricedReceiptsToCustomers",price);
		context.write(new IntWritable(customerID), t);
		
	}
}
