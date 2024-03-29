package urlCount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UrlCountReducer extends Reducer<Text, IntWritable, Text, IntWritable > {
	
	@Override
	public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, 
	InterruptedException {
		int finalSum = 0;
		for(IntWritable value: values){
			finalSum += value.get();
		}
		if(finalSum > 10){
			context.write(key, new IntWritable(finalSum));
		}
	}
}