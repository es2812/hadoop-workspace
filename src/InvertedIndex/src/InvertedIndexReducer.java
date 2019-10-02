import java.io.IOException;
import java.util.Map.Entry;
import java.util.TreeMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {
	@Override
	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		
		TreeMap<String,Integer> frequency = new TreeMap<String,Integer>();
		int cuentaActual = 0;
		for(Text value: values){
			if(frequency.containsKey(value.toString())){
				cuentaActual = frequency.get(value.toString())+1;
			}
			frequency.put(value.toString(), cuentaActual);
		}
		
		String valor = "";
		for(Entry<String, Integer> filenamecount: frequency.entrySet()){
			valor += filenamecount.getKey()+": "+filenamecount.getValue();
		}
		
		context.write(key, new Text(valor));
	}
}