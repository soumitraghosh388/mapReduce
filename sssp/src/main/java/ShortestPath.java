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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import java.net.URI;
import java.io.BufferedReader;
import java.io.InputStreamReader;

public class ShortestPath extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String[] split = value.toString().split("\\t");
			String node_key = split[0];
			String node_info = split[1];
			String[] node_inf_list = split[1].split(",");
			String adj_list = node_inf_list[0];
			String dist = node_inf_list[1];

			context.write(new Text(node_key), new Text(node_info));
			for(String node : adj_list.split("\\s+"))
			{
				String d = (dist.equals("inf")) ? dist : String.valueOf(Integer.parseInt(dist)+1);
				context.write(new Text(node), new Text(d));
			}

		}
	}
		
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
		{
			
			double d_min = Double.POSITIVE_INFINITY;
			//String[] d_list = new String();
			//int index = 0;
			String d_updated = "";
			//String d_temp = "";
			String d = "";
			String[] d_list;
			for(Text value:values) {
				//d_temp = value.toString();
				d_list = value.toString().split(",");
				
				if (d_list.length > 1){ 
					d_updated = value.toString();
					d = d_list[1]; 
				}
				else
					d = value.toString();
				if (d.equals("inf"))
					continue;
				else if (Integer.parseInt(d) < d_min) 
					d_min = Integer.parseInt(d);
			}
			String[] du_list = d_updated.split(",");
			String d_min_str = (Double.isInfinite(d_min)) ? "inf" : String.valueOf((int)d_min);
			String d_final = du_list[0] + "," + d_min_str;
			context.write(key, new Text(d_final));
		}
		
	}

	/*public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "ShortestPath");
		job.setJarByClass(ShortestPath.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}*/

	public int run(String[] args) throws Exception {
		//for(String str : args)
		//	System.out.println(str);
		if (args.length != 2) {
		    System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
		    ToolRunner.printGenericCommandUsage(System.err);
		    return -1;
		}

		Configuration conf = new Configuration();
		int iter = 2;
		int i = 0;
		boolean res = false;
		boolean isStop = false;
		Path inPath = new Path(args[0]);
		Path outPath = null;
		while(!isStop)
		{	
			outPath = new Path(args[1]+i);
			i++;
			Job job = new Job(conf, "ShortestPath");
			job.setJarByClass(ShortestPath.class);
			job.setMapperClass(Map.class);
			job.setReducerClass(Reduce.class);
			job.setNumReduceTasks(3);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileInputFormat.addInputPath(job, inPath);
			FileOutputFormat.setOutputPath(job, outPath);
			res = job.waitForCompletion(true);
			isStop = checkStopCriteria(outPath);
			inPath = outPath;
		}
		return res ? 0 : 1;
	}


	// Driver program
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new ShortestPath(), args);
		if (exitCode == 1) {
		    System.exit(exitCode);
		}
	}
	
	public boolean checkStopCriteria(Path inPath) throws Exception
	{
		Configuration con = new Configuration();
		FileSystem fs = FileSystem.get(inPath.toUri(), con);
		List<String> files = getFiles(inPath, fs);
		for (String file : files)
		{
			FSDataInputStream in = fs.open(new Path(file));
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String line = br.readLine();
			System.out.println("line : "+line);
			while(line!=null)
			{
				String[] split1 = line.split("\\t");
				String dist = split1[1].split(",")[1];
				System.out.println("dist :"+dist);
				if (dist.equals("inf"))
					return false;
				line = br.readLine();
				System.out.println("line : "+line);
			}
		}
		return true;
	}
	
	public List<String> getFiles(Path inPath, FileSystem fs) throws Exception
	{
		List<String> fileList = new ArrayList<String>();
		FileStatus[] fileStatus = fs.listStatus(inPath);
		for (FileStatus filest : fileStatus)
		{
			fileList.add(filest.getPath().toString());
		}
		return fileList;
	}
}






