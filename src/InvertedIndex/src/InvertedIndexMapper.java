import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class InvertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		
		FileSplit fileSplit = (FileSplit)context.getInputSplit();
		
		String fileName = fileSplit.getPath().getName();
		String text = value.toString();
		
		for(String word : text.split(" ")){
			context.write(new Text(word), new Text(fileName));
		}
	}
}