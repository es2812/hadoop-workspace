package hourCount;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import processor.Log;

public class HourCountMapper extends Mapper<Text, Log, Text, IntWritable> {
	
	@Override
	public void map(Text url, Log value, Context context)throws IOException, InterruptedException {
		String fecha = value.getFecha().toString();
		String año = fecha.split("/")[2]; 
		
		String tiempo = value.getHora().toString();
		String hora = tiempo.split(":")[0];
		
		if(año.equals("2010")){
			context.write(new Text(hora), new IntWritable(1));
		}
		
	}
}