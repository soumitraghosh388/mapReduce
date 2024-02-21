import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat; 
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import java.util.*;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;

public class Preprocess extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{

		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{

			String[] split=value.toString().split("\\t");

			String userId=split[0];
			String alist_dist = "";
			String dist = "inf";
			if (Integer.parseInt(userId) == 1)
				dist = "0";
			alist_dist = split[1] + "," + dist;
			
			context.write(new Text(userId),new Text(alist_dist));
			
		}
	}
		
	 /*public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "Preprocess");
		job.setJarByClass(Preprocess.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
		    ToolRunner.printGenericCommandUsage(System.err);
		    return -1;
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "Preprocess");
		job.setJarByClass(Preprocess.class);
		job.setMapperClass(Map.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true)? 0 : 1;
	}


	// Driver program
	public static void main(String[] args) throws Exception {
		int exitCode = ToolRunner.run(new Preprocess(), args);
		if (exitCode == 1) {
		    System.exit(exitCode);
		}
	}
}
