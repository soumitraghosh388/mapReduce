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
//package mutfriend;
public class FriendList extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, Text>
	{
		//Text user = new Text();
		//Text friends = new Text();
		
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
		{
			
			String[] split = value.toString().split("\\s+");
			String userId = split[0];
			String friend = split[1];
			context.write(new Text(userId), new Text(friend));
			context.write(new Text(friend), new Text(userId));
			

			/*Splitting each line into user and corresponding friend list*/
			//String[] split=value.toString().split("\\t");
			/*split[0] is user and split[1] is friend list */
			/*String userId=split[0];
			if(split.length==1) {
				return;
			}
			String[] friendIds=split[1].split(",");
			for(String friend : friendIds) {
				if(userId.equals(friend)) {
					continue;
				}
			String userKey = (Integer.parseInt(userId) < Integer.parseInt(friend))?userId + "," +friend : friend + ","+ userId;
			String regex="((\\b"+ friend + "[^\\w]+)|\\b,?" + friend + "$)";
			friends.set(split[1].replaceAll(regex, ""));
			user.set(userKey);
			context.write(user,friends);
			}*/
		}
	}
		
	public static class Reduce extends Reducer<Text, Text, Text, Text>
	{
		
		
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException,InterruptedException
		{
			
			StringBuilder friendsList = new StringBuilder();
			
			for(Text value:values) {
				friendsList.append(value.toString());
				friendsList.append(" ");
			}
			context.write(key, new Text(friendsList.toString()));

			/*String[] friendsList = new String[2];
			int index=0;
			
			for(Text value:values) {
				friendsList[index++] = value.toString();
			}
			String mutualFriends = matchingFriends(friendsList[0],friendsList[1]);
			if(mutualFriends != null && mutualFriends.length() != 0) {
				context.write(key, new Text(mutualFriends));
			}*/
		}
		
	}

	public int run(String[] args) throws Exception {
		//for(String str : args)
		//	System.out.println(str);
		if (args.length != 2) {
		    System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
		    ToolRunner.printGenericCommandUsage(System.err);
		    return -1;
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "FriendList");
		job.setJarByClass(FriendList.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(3);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		return job.waitForCompletion(true)? 0 : 1;
	}


	// Driver program
	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new FriendList(), args);
		if (exitCode == 1) {
		    System.exit(exitCode);
		}
	}
}
