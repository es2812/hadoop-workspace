import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class SocialNetwork {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: SocialNetwork <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();

		//job1 - obtains pairs of users that are not friends
		//and the number of common friends they share
		Job job1 = Job.getInstance(conf,"SocialNetwork");
		job1.setJarByClass(SocialNetwork.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));

		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setOutputPath(job1, new Path("pairs"));
		
		job1.setMapperClass(SocialNetworkPairsMapper.class);
		job1.setMapOutputKeyClass(Pair.class);
		job1.setMapOutputValueClass(Text.class);
				
		job1.setReducerClass(SocialNetworkPairsReducer.class);
		
		job1.setOutputKeyClass(Pair.class);
		job1.setOutputValueClass(IntWritable.class);
		
		if(!job1.waitForCompletion(true)){
			System.exit(1);
		}
		
		//job2 - for every user obtains the 10 other users with the highest number of common friends
		Job job2 = Job.getInstance(conf,"SocialNetwork");
		
		SequenceFileInputFormat.addInputPath(job2, new Path("pairs"));
		job2.setInputFormatClass(SequenceFileInputFormat.class);

		FileOutputFormat.setOutputPath(job2, new Path(args[1]));
		
		job2.setMapperClass(SocialNetworkMapper.class);
		job2.setReducerClass(SocialNetworkReducer.class);
		
		job2.setOutputKeyClass(IntWritable.class);
		job2.setOutputValueClass(Text.class);
				
		System.exit(job2.waitForCompletion(true) ? 0 : 1);
	}
}
