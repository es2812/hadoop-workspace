package mappers;


import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Tuple implements Writable {

	private Text type;
	private Text content;
	
	public Tuple(){
		this.type = new Text();
		this.content = new Text();
	}
	
	@Override
	public String toString(){
		return this.content.toString();
	}
	
	public Tuple(String type, String content) {
		this.type = new Text(type);
		this.content = new Text(content);
	}
	private void clear(){
		this.type = new Text();
		this.content = new Text();
	}
	@Override
	public void readFields(DataInput arg0) throws IOException {
		this.clear();
		
		this.type.readFields(arg0);
		this.content.readFields(arg0);
	}

	@Override
	public void write(DataOutput arg0) throws IOException {
		
		this.type.write(arg0);
		this.content.write(arg0);
	}

	public String getType() {
		return type.toString();
	}

	public void setType(String type) {
		this.type = new Text(type);
	}

	public String getContent() {
		return content.toString();
	}

	public void setContent(String content) {
		this.content = new Text(content);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((content == null) ? 0 : content.hashCode());
		result = prime * result + ((type == null) ? 0 : type.hashCode());
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
		Tuple other = (Tuple) obj;
		if (content == null) {
			if (other.content != null)
				return false;
		} else if (!content.equals(other.content))
			return false;
		if (type == null) {
			if (other.type != null)
				return false;
		} else if (!type.equals(other.type))
			return false;
		return true;
	}

}
