package processor;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class Log implements Writable{
	
	private Text ip;
	private Text fecha;
	private Text hora;
	private IntWritable status;
	
	public Log(){
		this.ip = new Text();
		this.fecha = new Text();
		this.hora = new Text();
		this.status = new IntWritable();
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.clear();
		
		this.ip.readFields(in);		
		this.fecha.readFields(in);		
		this.hora.readFields(in);
		this.status.readFields(in);
	}
	
	private void clear(){
		this.ip = new Text();
		this.fecha = new Text();
		this.hora = new Text();
		this.status = new IntWritable();
	}

	@Override
	public void write(DataOutput out) throws IOException {		
		this.ip.write(out);
		this.fecha.write(out);
		this.hora.write(out);		
		this.status.write(out);		
		
		this.clear();
	}
	
	@Override
	public String toString(){
		return this.ip.toString()+" "+this.fecha.toString()+" "+this.hora.toString()+" "+String.valueOf(this.status.get());
	}
	
	public Text getIp(){
		return this.ip;
	}
	
	public Text getFecha(){
		return this.fecha;
	}
	
	public Text getHora(){
		return this.hora;
	}
	
	public IntWritable getStatus(){
		return this.status;
	}
	
	public void setIp(String ip){
		this.ip = new Text(ip);
	}
		
	public void setFecha(String fecha){
		this.fecha = new Text(fecha);
	}
	
	public void setHora(String hora){
		this.hora = new Text(hora);
	}
	
	public void setStatus(int status){
		this.status = new IntWritable(status);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;

		result = prime * result + ((ip == null) ? 0 : ip.hashCode());
		result = prime * result + ((fecha == null) ? 0 : fecha.hashCode());
		result = prime * result + ((hora == null) ? 0 : hora.hashCode());
		result = prime * result + ((status == null) ? 0 : status.hashCode());
		
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
		Log other = (Log) obj;
		if (ip == null) {
			if (other.ip != null)
				return false;
		} else if (!ip.equals(other.ip))
			return false;
		
		if (fecha == null) {
			if (other.fecha != null)
				return false;
		} else if (!fecha.equals(other.fecha))
		
		if (hora == null) {
			if (other.hora != null)
				return false;
		} else if (!hora.equals(other.hora))
		
		if (status == null) {
			if (other.status != null)
				return false;
		} else if (!status.equals(other.status))
			return false;
		return true;
	}
	
}
