package poiTrainSet;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * key : time
 * value : poiid
 * @author coco1
 *
 */
public class poiMapper extends MapReduceBase implements
Mapper<LongWritable, Text, poiStatusIn,IntWritable> {
	private Text poiid = new Text();
	private Text time = new Text();
	private Text type = new Text();
	private DoubleWritable lat = new DoubleWritable();
	private DoubleWritable lon = new DoubleWritable();
	public void map(LongWritable key, Text value,
    OutputCollector<poiStatusIn,IntWritable> output, Reporter reporter)throws IOException {
		IntWritable one = new IntWritable(1);
		String line = value.toString();	
		System.out.println(line);
		String[] timePoiid = line.split(" ");
		for(int i = 0 ; i < 4 ; i++)
		{
		System.out.println(timePoiid[i]);
		}
		time.set(timeGet(timePoiid[4]));
		poiid.set(timePoiid[3]);
		lat.set(Double.parseDouble(timePoiid[0]));
		lon.set(Double.parseDouble(timePoiid[1]));
		type.set(timePoiid[2]);
		poiStatusIn pois = new poiStatusIn(poiid , time , lat , lon ,type);
    output.collect(pois,one);	
}
	@SuppressWarnings("deprecation")
	public String timeGet(String s){
		SimpleDateFormat sdf = new SimpleDateFormat();
		sdf.applyPattern("HH:mm:ss");
		Date day;
		int q = 0 ;
		try {
			day = sdf.parse(s);
			q = day.getHours();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return String.valueOf(q);
		
	}
}



