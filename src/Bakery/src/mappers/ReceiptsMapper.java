package mappers;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ReceiptsMapper extends Mapper<LongWritable, Text, IntWritable, Tuple> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		if(key.get() != 0){ //key=0: first row of the csv (headers)
			String line = value.toString().replaceAll("\\s","").replaceAll("\\t","");
			
			String[] fields = line.split(",");
			
			int id = Integer.parseInt(fields[0]);
			String date = fields[1];
			String customer = fields[2];

			Tuple t = new Tuple("Receipts",date+","+customer);
			context.write(new IntWritable(id), t);
		}
	}
}
