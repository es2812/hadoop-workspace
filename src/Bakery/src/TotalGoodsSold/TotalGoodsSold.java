package TotalGoodsSold;
import mappers.GoodsMapper;
import mappers.ItemsToGoodsMapper;
import mappers.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class TotalGoodsSold {
	public static void main(String[] args) throws Exception {

		if (args.length != 3) {
			System.err.println("Usage: TotalGoodsSold <goods path> <items path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		//job
		Job job = Job.getInstance(conf,"TotalGoodsSold");
		job.setJarByClass(TotalGoodsSold.class);
				
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, GoodsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, ItemsToGoodsMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Tuple.class);
		
		FileOutputFormat.setOutputPath(job, new Path(args[2]));
		
		job.setReducerClass(TotalGoodsSoldReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
