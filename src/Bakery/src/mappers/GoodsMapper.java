package mappers;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class GoodsMapper extends Mapper<LongWritable, Text, Text, Tuple> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		if(key.get() != 0){ //key=0: first row of the csv (headers)
			String line = value.toString().replaceAll("\\s","").replaceAll("\\t","");
			
			String[] fields = line.split(",");
			
			String id = fields[0];
			String flavour = fields[1];
			String category = fields[2];
			String price = fields[3];

			Tuple t = new Tuple("Goods",flavour+","+category+","+price);
			context.write(new Text(id), t);
		}
	}
}
