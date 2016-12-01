import java.io.IOException;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class ex3 {
	/*编写MapReduce，统计这两个文件

	/user/hadoop/mapred_dev_double/ip_time

	/user/hadoop/mapred_dev_double/ip_time_2

	当中，重复出现的IP的数量*/
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
		@SuppressWarnings("unused")
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			String filename = null;
			if(filename == "ip_time"){
				context.write(new Text("ip1"), new Text(str[0]));
			}else {
				context.write(new Text("ip2"), new Text(str[0]));
			}
		}		
	} 
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>{
		HashSet<String> set1 = new HashSet<>();
		HashSet<String> set2 = new HashSet<>();
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			if(key.equals("ip1")){
				for (Text text : values) {
					String ip = text.toString();
					set1.add(ip);
				}
			}else {
				for (Text text : values) {
					String ip = text.toString();
					set2.add(ip);
				}
			}		
		}
		public void cleanup(Context context)throws IOException, InterruptedException {
			set2.retainAll(set1);
			context.write(new Text("IPnums"), new IntWritable(set2.size()));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ex3");
		job.setJarByClass(ex3.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setNumReduceTasks(1);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
