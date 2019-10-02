package TotalSpent;
import java.io.IOException;

import mappers.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReceiptPricesReducer extends Reducer<IntWritable, Tuple, IntWritable, Text> {
		
	@Override
	public void reduce(IntWritable key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
		double finalPrice = 0.0;
		String receipt = "";
		System.out.println("NEW RECEIPT");
		for(Tuple value: values){
			if(value.getType().equals("Receipts")){
				receipt = value.getContent();
			}
			else if(value.getType().equals("PricedItemsToReceipts")){
				finalPrice += Double.parseDouble(value.getContent());
			}
		}
		
		context.write(key, new Text(receipt+","+String.valueOf(finalPrice)));
	}
	
}