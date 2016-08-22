package poiTrainSet;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
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
        Mapper<Text, Text, poiStatusIn,IntWritable> {
   		private Text poiid = new Text();
   		private Text time = new Text();
   		public void map(Text key, Text value,
            OutputCollector<poiStatusIn,IntWritable> output, Reporter reporter)throws IOException {
   			IntWritable one = new IntWritable(1);
   			String line = value.toString();	
   			String[] timePoiid = line.split(" ");
   			time.set(timePoiid[0]);
   			poiid.set(timePoiid[1]);
   			poiStatusIn pois = new poiStatusIn(poiid , time);
            output.collect(pois,one);	
    }
}

