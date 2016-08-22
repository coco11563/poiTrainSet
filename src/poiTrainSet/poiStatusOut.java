package poiTrainSet;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

public class poiStatusOut implements WritableComparable<poiStatusOut>{
	private Text poiid ;
	private IntWritable num ;
	/**
	 * 
	 * @param poiid
	 * @param num
	 */
	public poiStatusOut(Text poiid, IntWritable num)
	{
		this.setNum(num);
		this.setPoiid(poiid);
	}

	 /** 
     * 通过成员对象本身的write方法，序列化每一个成员对象到输出流中 
     * @param dataOutput 
     * @throws IOException 
     */  
    @Override  
    public void write(DataOutput dataOutput) throws IOException { 
    	poiid.write(dataOutput);
    	num.write(dataOutput);
    }  
    /** 
     * 同上调用成员对象本身的readFields方法，从输入流中反序列化每一个成员对象 
     * @param dataInput 
     * @throws IOException 
     */  
    @Override  
    public void readFields(DataInput dataInput) throws IOException {  
    	poiid.readFields(dataInput);  
    	num.readFields(dataInput);  
    }  
    @Override  
    public int hashCode() {  
        return poiid.hashCode()*419+num.hashCode();  
    }  
    
    @Override  
    public boolean equals(Object o) {  
        if(o instanceof poiStatusOut){  
        	poiStatusOut pois=(poiStatusOut)o;  
            return poiid.equals(pois.poiid) && num.equals(pois.num);  
        }  
        return false;  
    }  
    /** 
     * implements WritableComparable必须要实现的方法,用于比较  排序 
     * @param poiStatus 
     * @return 
     */  
    @Override  
    public int compareTo(poiStatusOut poistatus) {  
        int cmp = poiid.compareTo(poistatus.poiid);  
        if(cmp!=0){  
            return cmp;  
        }  
        return num.compareTo(poistatus.num);  
    }  
    
    /**
     * return poiid and num
     */   
    @Override  
    public String toString() {  
        return poiid+"\t"+num;  
    }  
    
	public IntWritable getNum() {
		return num;
	}

	public void setNum(IntWritable num) {
		this.num = num;
	}

	public Text getPoiid() {
		return poiid;
	}

	public void setPoiid(Text poiid) {
		this.poiid = poiid;
	}


}
