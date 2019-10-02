import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountByLetter {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: WordCountByLetter <input path> <output path>");
			System.exit(-1);
		}

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf,"WordCountByLetter");
		job.setJarByClass(WordCountByLetter.class);
		job.setJobName("Word Count by Letter");
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.setMapperClass(WordCountByLetterMapper.class);
		job.setReducerClass(WordCountByLetterReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}