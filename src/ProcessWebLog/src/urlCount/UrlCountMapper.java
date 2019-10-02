package urlCount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import processor.Log;

public class UrlCountMapper extends Mapper<Text, Log, Text, IntWritable> {
	
	@Override
	public void map(Text url, Log value, Context context)throws IOException, InterruptedException {
		
		context.write(url, new IntWritable(1));
	}
}