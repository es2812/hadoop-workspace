package urlCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class UrlCount {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: UrlCount <input path> <output path> ");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		
		//job1 - url count
		Job job1 = Job.getInstance(conf,"UrlCount");
		job1.setJarByClass(UrlCount.class);
		
		SequenceFileInputFormat.addInputPath(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(args[1]));

		job1.setInputFormatClass(SequenceFileInputFormat.class);
		
		job1.setMapperClass(UrlCountMapper.class);
		job1.setReducerClass(UrlCountReducer.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(IntWritable.class);
		
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}