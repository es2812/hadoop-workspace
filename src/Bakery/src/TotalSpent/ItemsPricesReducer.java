package TotalSpent;
import java.io.IOException;
import java.util.ArrayList;

import mappers.Tuple;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ItemsPricesReducer extends Reducer<Text, Tuple, Text, Text> {
	
	private String price;
	private ArrayList<String> items;
	
	@Override
	public void reduce(Text key, Iterable<Tuple> values, Context context) throws IOException, InterruptedException {
		price = "";
		items = new ArrayList<String>();
		
		for(Tuple value: values){
			if(value.getType().equals("Goods")){
				price = value.getContent().split(",")[2];
			}
			else if(value.getType().equals("ItemsToGoods")){
				items.add(value.getContent());
			}
		}
		
		for(String item :this.items){
			context.write(key, new Text(item+","+price));
		}
	}
	
}