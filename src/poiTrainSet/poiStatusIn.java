package poiTrainSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class poiStatusIn implements WritableComparable<poiStatusIn>{
	private Text poiid ;
	private Text time;
	private DoubleWritable lat;
	private DoubleWritable lon;
	private Text type;
	/**
	 * 
	 * @param poiid
	 * @param time
	 */
	public poiStatusIn(Text poiid, Text time,DoubleWritable lat , DoubleWritable lon , Text type)
	{
		this.setTime(time);
		this.setPoiid(poiid);
		this.setLat(lat);
		this.setLon(lon);
		this.setType(type);
	}
	public poiStatusIn()
	{
		this.setTime(new Text());
		this.setPoiid(new Text());
		this.setLat(new DoubleWritable());
		this.setLon(new DoubleWritable());
		this.setType(new Text());
	}

	 /** 
     * 通过成员对象本身的write方法，序列化每一个成员对象到输出流中 
     * @param dataOutput 
     * @throws IOException 
     */  
    @Override  
    public void write(DataOutput dataOutput) throws IOException { 
    	poiid.write(dataOutput);
    	time.write(dataOutput);
    }  
    /** 
     * 同上调用成员对象本身的readFields方法，从输入流中反序列化每一个成员对象 
     * @param dataInput 
     * @throws IOException 
     */  
    @Override  
    public void readFields(DataInput dataInput) throws IOException {  
    	poiid.readFields(dataInput);  
    	time.readFields(dataInput);  
    }  
    @Override  
    public int hashCode() {  
        return poiid.hashCode()*419+time.hashCode();  
    }  
    
    @Override  
    public boolean equals(Object o) {  
        if(o instanceof poiStatusIn){  
        	poiStatusIn pois=(poiStatusIn)o;  
            return poiid.equals(pois.poiid) && time.equals(pois.time);  
        }  
        return false;  
    }  
    /** 
     * implements WritableComparable必须要实现的方法,用于比较  排序 
     * @param poiStatus 
     * @return 
     */  
    @Override  
    public int compareTo(poiStatusIn poistatus) {  
        int cmp = poiid.compareTo(poistatus.poiid);  
        if(cmp!=0){  
            return cmp;  
        }  
        return time.compareTo(poistatus.time);  
    }  
    
    /**
     * return poiid and num
     */   
    @Override  
    public String toString() {  
        return time+"\t"+ poiid + "\t" + lat +"\t" + lon + "\t" + type;  
    }  
   

	public Text getPoiid() {
		return poiid;
	}

	public void setPoiid(Text poiid) {
		this.poiid = poiid;
	}

	public Text getTime() {
		return time;
	}

	public void setTime(Text time) {
		this.time = time;
	}
	public DoubleWritable getLat() {
		return lat;
	}
	public void setLat(DoubleWritable lat) {
		this.lat = lat;
	}
	public DoubleWritable getLon() {
		return lon;
	}
	public void setLon(DoubleWritable lon) {
		this.lon = lon;
	}
	public Text getType() {
		return type;
	}
	public void setType(Text type) {
		this.type = type;
	}
	
	


}
