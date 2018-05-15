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

/**
 * Created by ram on 10/1/16.
 */
public class MutualFriends {


    public static class Map
            extends Mapper<LongWritable, Text, Text, Text> {

        Text user = new Text();
        Text friends = new Text();


        public void map(LongWritable key, Text value, Context context
        ) throws IOException, InterruptedException {

            //split the line into user and friends
            String[] split = value.toString().split("\\t");
            //split[0] - user
            //split[1] - friendList
            String userid = split[0];
            if( split.length == 1 ) {
                return;
            }
            String[] others = split[1].split(",");
            for( String friend : others ) {

                if( userid.equals(friend) )
                    continue;

                String userKey = (Integer.parseInt(userid) < Integer.parseInt(friend) ) ? userid + "," + friend : friend + "," + userid;
                String regex = "((\\b" + friend + "[^\\w]+)|\\b,?" + friend + "$)";
                friends.set(split[1].replaceAll(regex, ""));
                user.set(userKey);
                context.write(user, friends);
            }

        }



    }
    public static class Reduce1 extends Reducer<Text,Text,Text,Text> {
		int count;
        private String findMatchingFriends( String list1, String list2 ) {

            if( list1 == null || list2 == null )
                return null;

            String[] friendsList1 = list1.split(",");
            String[] friendsList2 = list2.split(",");

            //use LinkedHashSet to retain the sort order
            LinkedHashSet<String> set1 = new LinkedHashSet<>();
            for( String user: friendsList1 ) {
                set1.add(user);
            }

            LinkedHashSet<String> set2 = new LinkedHashSet<>();
            for( String user: friendsList2 ) {
                set2.add(user);
            }

            //keep only the matching items in set1
            set1.retainAll(set2);

            return set1.toString().replaceAll("\\[|\\]","");
        }

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            String[] friendsList = new String[2];
            int i = 0;

            for( Text value: values ) {
                friendsList[i++] = value.toString();
            }
            ArrayList<String> KeyList = new ArrayList<>();

            

            String mutualFriends = findMatchingFriends(friendsList[0],friendsList[1]);

                //if( mutualFriends != null && mutualFriends.length() != 0 ) {
			Text copyDataFromMutualFriends = new Text(mutualFriends);
			String[] mutualFriends = copyDataFromMutualFriends.toString().split(",");
			IntWritable countFriends = new IntWritable();
			count =0;
			for(String mutualFriend : mutualFriends){
				count++;
			}
            System.out.println(key+":::: "+ new Text( mutualFriends ) );
            context.write(key, new Text( count.toString() + "\t" + mutualFriends ) );

                //}
            
            //System.out.println(key+" : "+mutualFriends);
        }	

    }
	
	public static class Map2 extends Mapper<Text, Text, NullWritable, Text> {
		//public PairValue countAndFriendList = new PairValue();
		public static TreeMap<CountMutualFriends, Text> countToList = new TreeMap<CountMutualFriends, Text>(new CountComparator());
		
		public void map2 (Text key, Text values, Context context)
		throws IOException, InterruptedException {
			String[] data = values.toString().split("\t");
			int count = Integer.parseInt(data[0]);
			countToList.put(new CountMutualFriends(count), new Text( new CountMutualFriends(count).toString() + "_" + key.toString() + "\t" + values.toString());
			Iterator<Entry<CountMutualFriends, Text>> iter = countToList.entrySet().iterator();
			Entry<CountMutualFriends, Text> entry = null;
			
			while(countToList.size() > 10){
				entry = iter.next();
				iter.remove();
			}
			for (Text t:countToList.values()) {
 
                context.write(NullWritable.get(), t);
 
            }
			//countFriends.set(count);
			//countAndFriendList.set(count, mutualFriends);
			//context.write(key, new Text(countFriends.toString() + "," + values);
		}
	}
	
	public static class CountMutualFriends {
		private int count;
		public int getCount(){
			return Reduce1.count;
		}
		public int setCount(){
			this.count =  Reduce1.count;
		}
		public CountMutualFriends(int count){
			super();
			this.count = count;
		}	
	}
	
	public static class CountComparator implements Comparator<CountMutualFriends> {
		@Override
		public int compare(CountMutualFriends c1, CountMutualFriends c2){
			if(c1.getCount() > c2.getCount()){
				return 1;
			}else{
				return -1;
			}	
		}
	}	
	public static class Reduce2 extends Reducer<NullWritable, Text, NullWritable, Text> {
		public static TreeMap<CountMutualFriends, Text> countToList = new TreeMap<CountMutualFriends, Text>(new CountComparator());
		
		public void reduce2 (NullWritable key, Iterable<Text> values,Context context)
		throws IOException, InterruptedException {
			
			for(Text value: values){
				String[] data = values.toString().split("_");
				if(values.toString().length() > 0 ){
					int count = Integer.parseInt(data[0]);
					countToList.put(new CountMutualFriends(count), new Text(key.toString() + "\t" + values.toString());
				}
			}	
				Iterator<Entry<CountMutualFriends, Text>> iter = countToList.entrySet().iterator();
				Entry<CountMutualFriends, Text> entry = null;
				
				while(countToList.size() > 10){
					entry = iter.next();
					iter.remove();
				}
				for (Text t:countToList.descendingMap().values()) {
	 
					context.write(NullWritable.get(), t);
	 
				}
		}	
	}
	
    // Driver program
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

        // get all args
        if (otherArgs.length != 2) {
            System.err.println("Usage: MutualFriends <FriendsFile> <output>");
            System.exit(2);
        }


        // create a job with name "MutualFriends"
        Job job = new Job(conf, "MutualFriends");
        job.setJarByClass(MutualFriends.class);
		job.setNumReduceTasks(1);
        job.setMapperClass(Map1.class);
        job.setReducerClass(Reduce1.class);
// uncomment the following line to add the Combiner job.setCombinerClass(Reduce.class);
// set output key type
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
// set output value type
        job.setOutputValueClass(Text.class);
//set the HDFS path of the input data
        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        //FileInputFormat.addInputPath(job, new Path(otherArgs[1]));
// set the HDFS path for the output
        Path out = new Path(otherArgs[1]);
        out.getFileSystem(conf).delete(out,true);
        FileOutputFormat.setOutputPath(job, out);
//Wait till job completion
        System.exit(job.waitForCompletion(true) ? 0 : 1);
		
		
		Configuration conf2 = getConf();

		Job job2 = Job.getInstance(conf2, "Join");
		job2.setJarByClass(MutualFriends.class);

		Path secondInput = new Path(args[2]);
		Path finalOutput = new Path(args[3]);

		//MultipleInputs.addInputPath(job2, out, KeyValueTextInputFormat.class, MutualFriends.Map2.class);
		MultipleInputs.addInputPath(job2, secondInput, TextInputFormat.class, MutualFriends.Map2.class);
		//job2.setPartitionerClass(MyJob.MyPartition.class);

		FileOutputFormat.setOutputPath(job2, finalOutput);
		job2.setReducerClass(MutualFriends.Reduce2.class);
		job2.setOutputFormatClass(TextOutputFormat.class);

		job2.setMapOutputKeyClass(NullWritable.class);
		job2.setMapOutputValueClass(Text.class);

		job2.setOutputKeyClass(NullWritable.class);
		job2.setOutputValueClass(Text.class);

		System.exit(job2.waitForCompletion(true)?0:1);

		return 0;
    }
}

