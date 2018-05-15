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

public class MinMaxMedian{
	
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
	
	public static class Combiner extends Reducer<IntWritable, IntWritable, PairKey, Text> 
	{
		Text result = new Text();
		PairKey obj = new PairKey();
		int keyOfPair = 1;
		public void reduce(IntWritable key, Iterable<IntWritable> values, Context context)
		throws IOException, InterruptedException {
			int sum = 0, count = 0, sumSquare = 0;
			for(int num: values){
				count++;
				sum += num;
				sumSquare += (num * num);
				obj.set(keyOfPair, num);
			}
			String resultCountSumSquare = count.toString() + "," + sum.toString();
			
			result.set(resultCountSumSquare);
			context.write(obj,result);
		}
	}
	public static class PairKey implements WritableComparable<PairKey> {
		private int key;
		private int num;
		@Override
		public void readFields(DataInput in)throws IOException{
			key = 1;
			num = in.readInt();
		}
		@Override
		public void write(DataOutput out)throws IOException{
			out.writeInt(key);
			out.writeInt(num);
		}
		public int getKey(){
			return this.key;
		}
		public int getNum(){
			return this.num;
		}
		public void set(int key, int num){
			this.key = key;
			this.num = num;
		}
		@Override
		public int compareTo(PairKey pairKey){
			int comapareValue = this.key.compareTo(pairKey.getKey());
			if(comapareValue == 0){
				compareValue = num.compareTo(pairKey.getNum());
			}
			return comapareValue;
		}	
	}
	public static class NumPartitioner extends Partitioner<PairKey, Text>{
		@Override
		public int getPartition (PairKey pairKey, Text text, int numPartitios){
				return pairKey.getKey().hashCode() % numPartitios;
		}
	}	
	public static class GroupingComparator extends WritableComparable {
		public GroupingComparator() {
			super(PairKey.class, true);
		}
		@Override
		public int compare(WritableComparable num1, WritableComparable num2){
			PairKey pairKey1 = (PairKey)num1;
			PairKey pairKey2 = (PairKey)num2;
			return pairKey1.getKey().compareTo(pairKey2.getKey());
		}
	}
	public static class Reduce extends Reducer<PairKey, Text, IntWritable, Text> 
	{
		PairWritable meanVariance = new PairWritable();
		public void reduce(PairKey key, Iterable<Text> values, Context context)
		throws IOException, InterruptedException {
			int min = 0, max = 0,i=0;
			String[] result = values.split(",");
			int count = Integer.parseInt(result[0]);
			int sum = Integer.parseInt(result[1]);
			for(PairKey num:key){
				if(i==0){
					min = num.getNum();
				}	
				i++;
				if(i== (count - 1)){
					max = num.getNum();
				}		
			}
				
				
				int median = sum / 2;
				
				//meanVariance.set(mean, variance);
				context.write(key,new Text("min=" + min + ", max=" + max + "median=" + median));
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