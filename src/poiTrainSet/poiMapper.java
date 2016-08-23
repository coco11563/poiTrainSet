package poiTrainSet;

import java.io.IOException;

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
		time.set(timePoiid[0]);
		poiid.set(timePoiid[1]);
		lat.set(Double.parseDouble(timePoiid[2]));
		lon.set(Double.parseDouble(timePoiid[3]));
		poiStatusIn pois = new poiStatusIn(poiid , time , lat , lon );
    output.collect(pois,one);	
}
}



