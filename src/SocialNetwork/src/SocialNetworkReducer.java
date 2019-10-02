import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class SocialNetworkReducer extends Reducer<IntWritable, Text, IntWritable, Text> {

	@Override
	public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {		
		int user;
		int count;
		String[] line;
		
		int[] top10counts = new int[10];
		int[] top10users = new int[10];
		for(int i=0;i<10;i++){
			top10counts[i] = -1;
			top10users[i] = -1;
		}
	
		for(Text value: values){
			line = value.toString().split(":");
			
			user = Integer.parseInt(line[0]);
			count = Integer.parseInt(line[1]);
			
			for(int i=0;i<10;i++){
				if(count > top10counts[i]){
					//we have found a maximum in position i
					//we move all counts and users after i down one position
					for(int j=9;j>i;j--){
						top10counts[j] = top10counts[j-1];
						top10users[j] = top10users[j-1];
					}
					//we store the new count and user
					top10counts[i] = count;
					top10users[i] = user;
					
					break;
				}
				else if(count == top10counts[i]){
					//sort by user ascending
					if(user < top10users[i]){
						//we have found a minimum in position i
						//we move all counts and users after i down one position
						for(int j=9;j>i;j--){
							top10counts[j] = top10counts[j-1];
							top10users[j] = top10users[j-1];
						}
						//we store the new count and user
						top10counts[i] = count;
						top10users[i] = user;
						break;
					}
				}
			}
		}
		
		String result = "";
		for(int k=0;k<10;k++){
			if(top10counts[k]!=-1){
				if(k==0){
					result = String.valueOf(top10users[k]);
				}
				else{
					result += ","+String.valueOf(top10users[k]);
				}
			}
			else {
				break;
			}
		}
		context.write(key, new Text(result));
	}
}