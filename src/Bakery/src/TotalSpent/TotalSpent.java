package TotalSpent;
import mappers.CustomersMapper;
import mappers.GoodsMapper;
import mappers.ItemsToGoodsMapper;
import mappers.PricedItemsToReceiptsMapper;
import mappers.PricedReceiptsToCustomersMapper;
import mappers.ReceiptsMapper;
import mappers.Tuple;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class TotalSpent {
	public static void main(String[] args) throws Exception {

		if (args.length != 5) {
			System.err.println("Usage: TotalSpent <items path> <goods path> <receipts path> <customers path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		//job1 - calculates prices of each item
		Job job = Job.getInstance(conf,"TotalSpent");
		job.setJarByClass(TotalSpent.class);
				
		MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, ItemsToGoodsMapper.class);
		MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, GoodsMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Tuple.class);
		
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		SequenceFileOutputFormat.setOutputPath(job, new Path("aux/pricePerItem"));
		
		job.setReducerClass(ItemsPricesReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		if(!job.waitForCompletion(true)){
			System.exit(1);
		}

		//job2 - calculates spent on each receipt
		Job job2 = Job.getInstance(conf,"TotalSpent");
		job2.setJarByClass(TotalSpent.class);
				
		MultipleInputs.addInputPath(job2, new Path("aux/pricePerItem"), SequenceFileInputFormat.class, PricedItemsToReceiptsMapper.class);
		MultipleInputs.addInputPath(job2, new Path(args[3]), TextInputFormat.class, ReceiptsMapper.class);

		job2.setMapOutputKeyClass(IntWritable.class);
		job2.setMapOutputValueClass(Tuple.class);

		job2.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job2, new Path("aux/pricePerReceipt"));
		
		job2.setReducerClass(ReceiptPricesReducer.class);
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
		
		if(!job2.waitForCompletion(true)){
			System.exit(1);
		}
			
		//job3 - calculates spent on every receipt for each customer
		Job job3 = Job.getInstance(conf,"TotalSpent");
		job3.setJarByClass(TotalSpent.class);
				
		MultipleInputs.addInputPath(job3, new Path("aux/pricePerReceipt"), SequenceFileInputFormat.class, PricedReceiptsToCustomersMapper.class);
		MultipleInputs.addInputPath(job3, new Path(args[4]), TextInputFormat.class, CustomersMapper.class);

		job3.setMapOutputKeyClass(IntWritable.class);
		job3.setMapOutputValueClass(Tuple.class);
		
		FileOutputFormat.setOutputPath(job3, new Path(args[0]));
		
		job3.setReducerClass(TotalSpentReducer.class);
		
		job3.setOutputKeyClass(IntWritable.class);
		job3.setOutputValueClass(Text.class);

		System.exit(job3.waitForCompletion(true) ? 0 : 1);
		
	}
}
