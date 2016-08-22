package poiTrainSet;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class poiReducer extends MapReduceBase implements
Reducer<poiStatusIn, IntWritable, Text, poiStatusOut> {
public void reduce(poiStatusIn in, Iterator<IntWritable> num,
    OutputCollector<Text, poiStatusOut> output, Reporter reporter)
    throws IOException {
	int sum = 0;
     while (num.hasNext()) {
         sum += num.next().get();
     }
     IntWritable all = new IntWritable(sum);
     poiStatusOut out = new poiStatusOut(in.getPoiid(), all);
	output.collect(in.getTime(), out);
}
}