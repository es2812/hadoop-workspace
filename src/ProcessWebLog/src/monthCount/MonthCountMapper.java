package monthCount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import processor.Log;

public class MonthCountMapper extends Mapper<Text, Log, Text, IntWritable> {
	
	@Override
	public void map(Text url, Log value, Context context)throws IOException, InterruptedException {
		String fecha = value.getFecha().toString();
		String mes = fecha.split("/")[1];
		String año = fecha.split("/")[2]; 
		if(año.equals("2010")){
			context.write(new Text(mes), new IntWritable(1));
		}
		
	}
}