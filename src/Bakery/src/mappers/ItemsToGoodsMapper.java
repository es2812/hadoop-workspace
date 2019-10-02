package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ItemsToGoodsMapper extends Mapper<LongWritable, Text, Text, Tuple> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		if(key.get() != 0){ //key=0: first row of the csv (headers)
			String line = value.toString().replaceAll("\\s","").replaceAll("\\t","");
			
			String[] fields = line.split(",");
			
			String receiptId = fields[0];
			String place = fields[1];
			
			String goodId = fields[2];
			
			Tuple t = new Tuple("ItemsToGoods", receiptId+","+place);
			
			context.write(new Text(goodId), t);
		}
	}
}