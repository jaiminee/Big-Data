import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.*;

public class MeanVariance{
	
	public static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> 
	{
		Text key = new Text();
		Text valueNumber = new Text();
		IntWritable one = new IntWritable(1);
		public void map(LongWritable key, Text value, Context context)
		throws IOException, InterruptedException {
			int num = Integer.parseInt(valueNumber);
			key.set(one);
			valueNumber.set(num);
			context.write(one,num);
		}
	}
	
	public static class Combiner extends Reducer<IntWritable, IntWritable, IntWritable, Text> 
	{
		Text result = new Text();
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0, count = 0, sumSquare = 0;
			for(int num: values){
				count++;
				sum += num;
				sumSquare += (num * num);
			}
			String resultCountSumSquare = count.toString() + "," + sum.toString() + "," + sumSquare.toString();
			result.set(resultCountSumSquare);
			context.write(key,result);
		}
	}
	
	public static class Reduce extends Reducer<IntWritable, Text, IntWritable, Text> 
	{
		PairWritable meanVariance = new PairWritable();
		public void reduce(IntWritable key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
				String[] result = values.split(",");
				int count = Integer.parseInt(result[0]);
				int sum = Integer.parseInt(result[1]);
				int sumSquare = Integer.parseInt(result[2]);
				int mean = sum / count;
				int variance = (sumSquare / count) - (mean * mean);
				//meanVariance.set(mean, variance);
				context.write(key,new Text("mean=" + mean + ", variance=" + variance));
		}
		
	}
	public static void main(String[] args){
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <FriendsFile> <output>");
            System.exit(2);
        }

		
		Job job = Job.getInstance(conf, "MeanVariance");
        job.setJarByClass(MeanVariance.class);
        job.setMapperClass(Map.class);
		job.setCombinerClass(Combiner.class);
        job.setReducerClass(Reduce.class);

        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		Path out = new Path(otherArgs[1]);
        out.getFileSystem(conf).delete(out,true);
        //FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0:1);
	}
}