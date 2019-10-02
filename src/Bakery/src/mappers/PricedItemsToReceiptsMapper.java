package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PricedItemsToReceiptsMapper extends Mapper<Text, Text, IntWritable, Tuple> {
	
	@Override
	public void map(Text key, Text value, Context context)throws IOException, InterruptedException {
		
		//FORMAT: key: 		GoodsID
		//		  value:	ReceiptID, Place, Price
		
		String[] fields = value.toString().split(",");
		
		int receiptId = Integer.parseInt(fields[0]);
		String price = fields[2];
		
		Tuple t = new Tuple("PricedItemsToReceipts",price);
		
		context.write(new IntWritable(receiptId), t);
	
	}
}
