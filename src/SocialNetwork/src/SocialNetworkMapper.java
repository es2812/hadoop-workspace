
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialNetworkMapper extends Mapper<Pair, IntWritable, IntWritable, Text> {
	
	@Override
	public void map(Pair key, IntWritable value, Context context)throws IOException, InterruptedException {
		//number of common friends is a symmetric value
		context.write(new IntWritable(key.getPairFirst()), new Text(String.valueOf(key.getPairSecond()) + ":" + value.toString()));
		context.write(new IntWritable(key.getPairSecond()), new Text(String.valueOf(key.getPairFirst()) + ":" + value.toString()));
	}
}
