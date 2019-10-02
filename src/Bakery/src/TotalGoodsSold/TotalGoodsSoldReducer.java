package TotalGoodsSold;
import java.io.IOException;

import mappers.Tuple;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TotalGoodsSoldReducer extends Reducer<Text, Tuple, Text, IntWritable> {

	@Override
	public void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {		
		int finalSum = 0;
		String[] good = null;
		
		for(Tuple value: values){
			if(value.getType().equals("Goods")){
				good = value.getContent().split(",");
			}else if(value.getType().equals("ItemsToGoods")){
				finalSum += 1;
			}
		}
		
		context.write(new Text(key.toString()+","+good[0]+","+good[1]), new IntWritable(finalSum));
	}
}