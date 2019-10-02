package processor;
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.regex.*;

public class ProcessWebLogMapper extends Mapper<LongWritable, Text, Text, Log> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String line = value.toString();
				
		Pattern pattern = Pattern.compile("^(\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}).*\\[(\\d{2}/\\w{3}/\\d{4}):(\\d{2}:\\d{2}:\\d{2}).*\\] "
				+ "\"[A-Z]+ (.*) HTTP.*\" (\\d{3}).*");
		Matcher matcher = pattern.matcher(line);
		if(matcher.matches()){
			String ip = matcher.group(1);
			String date = matcher.group(2);
			String time = matcher.group(3);
			String url = matcher.group(4);
			int status = Integer.parseInt(matcher.group(5));
			
			if(status/100 == 2){
				Log log = new Log();
				
				log.setIp(ip);
				log.setFecha(date);
				log.setHora(time);
				log.setStatus(status);
				
				context.getCounter("StatusCounters","OK").increment(1);
				context.write(new Text(url), log);
				
			}
			else if(status/100 == 3)
				context.getCounter("StatusCounters","RETRY").increment(1);
			else if(status/100 == 4)
				context.getCounter("StatusCounters","ERROR").increment(1);
		}
		else
			context.getCounter("StatusCounters","NOMATCH").increment(1);
		
	}
}
