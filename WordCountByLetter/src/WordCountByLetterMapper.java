import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class WordCountByLetterMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		String line = value.toString();
		char firstCharacter;
        int pointer;
        
		for(String word : line.split(" ")){
			
			pointer = 0;
			firstCharacter = '?';
			
			while(pointer < word.length() & (firstCharacter < 'a' || firstCharacter > 'z')){
				firstCharacter = word.toLowerCase().charAt(pointer);
				pointer += 1;
			}
			
			if( (firstCharacter >= 'a' && firstCharacter <= 'z') ){ //if character is a letter we write it
				context.write(new Text(Character.toString(firstCharacter)), new IntWritable(1));
			}
		}
	}
}