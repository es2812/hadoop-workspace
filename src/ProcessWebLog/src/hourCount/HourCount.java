package hourCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class HourCount {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: MonthCount <input path> <output path> ");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		//job1 - month count
		Job job1 = Job.getInstance(conf,"MonthCount");
		job1.setJarByClass(HourCount.class);
		
		SequenceFileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		
		job1.setMapperClass(HourCountMapper.class);
		job1.setReducerClass(monthCount.MonthCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}