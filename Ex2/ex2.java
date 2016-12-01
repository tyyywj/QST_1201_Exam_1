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


public class ex2 {
	//编写MapReduce，统计/user/hadoop/mapred_dev/ip_time 中去重后的IP数，越节省性能越好
	public static class Map extends Mapper<LongWritable, Text, Text, Text>{
	
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			String[] str = value.toString().split("\t");
			String ip = "ip";
			context.write(new Text(ip), new Text(str[0]));
		}		
	} 
	
	public static class Reduce extends Reducer<Text, Text, Text, IntWritable>{
		HashSet<String> set = new HashSet<>();
		String IP = "ipnum";
		public void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			for (Text text : values) {
				String str = text.toString();
				set.add(str);
			}
		}
		public void cleanup(Context context)throws IOException, InterruptedException {
			context.write(new Text(IP), new IntWritable(set.size()));
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "ex2");
		job.setJarByClass(ex2.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
	}
}
