
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class SocialNetworkPairsMapper extends Mapper<LongWritable, Text, Pair, Text> {
	
	@Override
	public void map(LongWritable key, Text value, Context context)throws IOException, InterruptedException {
		//writes all combinations of pairs "related" users as keys, defining related as being either direct friends, or being on a friend list together
		//direct friends are written with the value "A" (amigos), and pairs that are found together on the list as "C" (comunes). lastly, if the user doesn't
		//have any friends it will appear with an "NA" value.
		//the WritableComparable type Pair is stored as the lowest id first and highest id second
		String[] line;
		line = value.toString().split("\t");
		
		int user = Integer.parseInt(line[0]);
		String[] friends;
		Pair pair;
		if(line.length > 1){
			friends = line[1].split(",");
			for(String friend: friends){
				pair = new Pair();
				pair.setPair(user, Integer.parseInt(friend));
				context.write(pair,new Text("A"));					
			}
			
			for(int i=0;i<friends.length;i++){
				for(int j=i+1;j<friends.length;j++){
					pair = new Pair();
					pair.setPair(Integer.parseInt(friends[i]), Integer.parseInt(friends[j]));
					context.write(pair,new Text("C"));
				}
			}
		}else{
			pair = new Pair();
			pair.setPair(user, user);
			context.write(pair,new Text("NA"));
		}
				
	}
}
