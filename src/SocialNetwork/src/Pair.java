import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

public class Pair implements WritableComparable<Pair> {

	private IntWritable first;
	private IntWritable second;
	
	public Pair(){
		this.first = new IntWritable();
		this.second = new IntWritable();
	}
	
	public void setPair(int one, int other){
		if(one < other){
			this.first = new IntWritable(one);
			this.second = new IntWritable(other);
		}else{
			this.first = new IntWritable(other);
			this.second = new IntWritable(one);
		}
		
	}
	
	public int getPairFirst(){
		return this.first.get();
	}

	public int getPairSecond(){
		return this.second.get();
	}
	
	private void clear(){
		this.first = new IntWritable();
		this.second = new IntWritable();
	}
	
	@Override
	public void readFields(DataInput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.clear();
		
		this.first.readFields(arg0);
		this.second.readFields(arg0);
	}
	@Override
	public void write(DataOutput arg0) throws IOException {
		// TODO Auto-generated method stub
		this.first.write(arg0);
		this.second.write(arg0);
	}

	@Override
	public int compareTo(Pair o) {
		// TODO Auto-generated method stub
		if(this.getPairFirst() < o.getPairFirst()){
			return -1;
		}else if(this.getPairFirst() > o.getPairFirst()){
			return 1;
		}else if(this.getPairSecond() < o.getPairSecond()){
			return -1;
		}else if(this.getPairSecond() > o.getPairSecond()){
			return 1;
		}
		else
			return 0;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((this.first == null) ? 0 : this.first.hashCode());
		result = prime * result + ((this.second == null) ? 0 : this.second.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Pair other = (Pair) obj;
		if (this.first == null) {
			if (other.first != null)
				return false;
		} else if (!this.first.equals(other.first))
			return false;
		if (this.second == null) {
			if (other.second != null)
				return false;
		} else if (!this.second.equals(other.second))
			return false;
		return true;
	}
	
	@Override
	public String toString(){
		return "("+this.first.toString()+", "+this.second.toString()+")";
	}
	
	
}
