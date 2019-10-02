import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialNetworkPairsReducer extends Reducer<Pair, Text, Pair, IntWritable> {
	
	@Override
	public void reduce(Pair key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		int timeCommon = 0;
		
		for(Text value: values){
			if(value.toString().equals("A")){
				//pair are friends, this automatically means they can't be recommended to each other
				timeCommon = -1; //writing a record with -1 means even if an user doesn't have any common friends that aren't already their friends, they'll show up on the list
				break;
			}else if(value.toString().equals("C")){
				timeCommon += 1;
			}else if(value.toString().equals("NA")){
				timeCommon = -1;
			}
		}
		context.write(key,new IntWritable(timeCommon));	
	}
}