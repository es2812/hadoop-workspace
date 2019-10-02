package processor;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

public class ProcessWebLog {
	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: ProcessWebLog <input path> <output path>");
			System.exit(-1);
		}
		
		Configuration conf = new Configuration();
		//Log processing and writing into SequenceFile
		Job job1 = Job.getInstance(conf,"ProcessWebLog");
		job1.setJarByClass(ProcessWebLog.class);
		
		FileInputFormat.addInputPath(job1, new Path(args[0]));
	
		job1.setMapperClass(ProcessWebLogMapper.class);
		
		job1.setOutputKeyClass(Text.class);
		job1.setOutputValueClass(Log.class);
		
		//compression of the output file
		job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job1, true);
//		SequenceFileOutputFormat.setOutputCompressorClass(job1, BZip2Codec.class);
		SequenceFileOutputFormat.setOutputCompressionType(job1, CompressionType.BLOCK);
		SequenceFileOutputFormat.setOutputPath(job1,  new Path(args[1]));

				
		System.exit(job1.waitForCompletion(true) ? 0 : 1);
	}
}
