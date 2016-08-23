package poiTrainSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class poiMainClass {
	public static void main(String[] args) throws Exception {
	    JobConf conf = new JobConf(poiMainClass.class);
	    conf.setJobName("poicount");
	    

	    conf.setOutputKeyClass(poiStatusIn.class);
	    conf.setOutputValueClass(IntWritable.class);
	    
	    conf.setMapperClass(poiMapper.class);
	    conf.setCombinerClass(poiReducer.class);
	    conf.setReducerClass(poiReducer.class);

	    conf.setInputFormat(TextInputFormat.class);
	    conf.setOutputFormat(TextOutputFormat.class);

	    FileInputFormat.setInputPaths(conf, new Path("hdfs://192.168.1.30:9000/usr/input/poi"));
	    FileOutputFormat.setOutputPath(conf, new Path("hdfs://192.168.1.30:9000/output/poi"));

	    JobClient.runJob(conf);
	}
}

