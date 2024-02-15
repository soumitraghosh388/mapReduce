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
public class MutualFriends extends Configured implements Tool{

	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		Text user = new Text();
		Text friends = new Text();
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException{
			/*Splitting each line into user and corresponding friend list*/
			String[] split=value.toString().split("\\t");
			/*split[0] is user and split[1] is friend list */
			String userId=split[0];
			if(split.length==1) {
				return;
			}
			String[] friendIds=split[1].trim().split("\\s+");
			for(String friend : friendIds) {
				if(userId.equals(friend)) {
					continue;
				}
				String userKey = (Integer.parseInt(userId) < Integer.parseInt(friend))?userId + " " +friend : friend + " "+ userId;
				//String regex="((\\b"+ friend + "[^\\w]+)|\\b,?" + friend + "$)";
				//friends.set(split[1].replaceAll(regex, ""));
				friends.set(split[1]);
				user.set(userKey);
				context.write(user,friends);
			}
		}
	}
		
	public static class Reduce
			extends Reducer<Text, Text, Text, Text>{
		
		private String matchingFriends(String firstList, String secondList) {
						
			if(firstList == null || secondList == null) {
				return null;
			}
			
			String[] list1=firstList.split("\\s+");
			String[] list2=secondList.split("\\s+");
			
			LinkedHashSet<String> firstSet = new LinkedHashSet();
			for(String  user: list1) {
				firstSet.add(user);
			}
			/*Retaining sort order*/
			LinkedHashSet<String> secondSet = new LinkedHashSet();
			for(String  user: list2) {
				secondSet.add(user);
			}
			firstSet.retainAll(secondSet);
			/*Keeping only the matched friends*/
			ArrayList<String> al = new ArrayList<>(firstSet);
			Collections.sort(al);
			return al.toString().replaceAll("\\[|\\]", "").replaceAll("\\,", " ");
		}
		
		public void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException,InterruptedException{
			
			String[] friendsList = new String[2];
			int index=0;
			
			for(Text value:values) {
				friendsList[index++] = value.toString();
			}
			String mutualFriends = matchingFriends(friendsList[0],friendsList[1]);
			if(mutualFriends != null && mutualFriends.length() != 0) {
				context.write(key, new Text(mutualFriends));
			}
		}
		
	}

	public int run(String[] args) throws Exception {
		if (args.length != 2) {
		    System.err.printf("Usage: %s [generic options] <input> <output>\n", getClass().getSimpleName());
		    ToolRunner.printGenericCommandUsage(System.err);
		    return -1;
		}

		Configuration conf = new Configuration();

		Job job = new Job(conf, "mutualfriends");
		job.setJarByClass(MutualFriends.class);
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
		int exitCode = ToolRunner.run(new MutualFriends(), args);
		if (exitCode == 1) {
		    System.exit(exitCode);
		}
	}
}
