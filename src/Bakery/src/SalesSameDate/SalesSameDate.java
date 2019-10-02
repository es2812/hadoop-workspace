package SalesSameDate;
import mappers.CustomersMapper;
import mappers.ReceiptsToCustomersMapper;
import mappers.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SalesSameDate {
	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: SalesSameDate <customers path> <receipts path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf,"SalesSameDate");
		job.setJarByClass(SalesSameDate.class);
				
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, CustomersMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ReceiptsToCustomersMapper.class);

		job.setMapOutputKeyClass(IntWritable.class);
		job.setMapOutputValueClass(Tuple.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(SalesSameDateReducer.class);
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
