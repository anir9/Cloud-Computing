
import java.io.IOException;
import java.util.*;
import java.util.PriorityQueue;
//import javafx.util.Pair;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.*;
import org.apache.hadoop.mapreduce.lib.output.*;
import org.apache.hadoop.util.*;
//import org.w3c.dom.Text;
//import org.apache.hadoop.hbase.util.*;

public class WordCount1 {

	public static class Node {
		String key;
		int value;

		public Node(String key, int value) {
			this.key = key;
			this.value = value;
		}
	}

	public static class Map extends Mapper<Object, Text, Text, IntWritable> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
    //private String tokens = "[_|$#<>\^=\[\]\/\\,;,.\-:()?!"']";

		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				context.write(word, one);
        //System.out.println(word + " " + one);
			}
       //System.out.println();
		}
	}

	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		TreeMap<Integer, List<String>> treeMap = new TreeMap<>((a,b) -> b-a);
		HashMap<String, Integer> map = new HashMap<>();

		public void reduce(Text key, Iterable<IntWritable> valueList, Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			Iterator<IntWritable> values = valueList.iterator();
			while (values.hasNext()) {
				sum += values.next().get();
			}
			map.put(key.toString(), map.getOrDefault(key.toString(), 0)+sum);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			for (String name: map.keySet()){
				int count = map.get(name);
				//String name = entry.getKey()
        List<String> list = treeMap.getOrDefault(count, new ArrayList<>());
				list.add(name);
				treeMap.put(count, list);
				//context.write(new IntWritable(count), new Text(name));
			}
			
			int counter = 0;
			for(int count : treeMap.keySet()) {
				List<String> list = treeMap.get(count);
				for(String word : list) {
					if(counter >= 100) break;
					context.write(new Text(word), new IntWritable(count));
          //System.out.println(word + " " + count);
					counter++;
				}
			}
      //System.out.println();
		}
	}

	/*public static class Map2 extends Mapper<Text, IntWritable, IntWritable, Text> {

		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();

		public void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
			String k = key.toString();
			int v = value.get();
			k = k + '\t' + v;
			context.write(one, new Text(k));
		}
	}

	public static class Reduce2 extends Reducer<IntWritable, Text, Text, IntWritable> {
		public void reduce(IntWritable key, Iterable<Text> valueList, Context context)
				throws IOException, InterruptedException {
			PriorityQueue<Node> pq = new PriorityQueue<>((a, b) -> a.value - b.value);
			Iterator<Text> values = valueList.iterator();
			while (values.hasNext()) {
				Text text = values.next();
				String[] value = text.toString().split("\t");
				String word = value[0];
				int count = Integer.parseInt(value[1]);
				Node toAdd = new Node(word, count);
				if (pq.size() < 100) {
					pq.add(toAdd);
				} else if (pq.peek().value < count) {
					pq.poll();
					pq.add(toAdd);
				}
			}

			while (!pq.isEmpty()) {
				Node value = pq.poll();
				String word = value.key;
				int count = value.value;
				context.write(new Text(word), new IntWritable(count));
			}
		}
	}*/

	public static void main(String[] args) throws Exception {

		if (args.length != 2) {
			System.err.println("Usage: wordcount <in> <out>");
			System.exit(2);
		}

		Job job = Job.getInstance(new Configuration());
		job.setJobName("wordcount");
		job.setJarByClass(WordCount1.class);

		job.setMapperClass(Map.class);
		//job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.submit();
		job.waitForCompletion(true);

//    Job job2 = Job.getInstance(new Configuration());
//    job2.setJobName("wordcount2");
//    job2.setJarByClass(WordCount1.class);
//
//    job2.setMapperClass(Map2.class);
//    job2.setCombinerClass(Reduce2.class);
//    job2.setReducerClass(Reduce2.class);
//
//    job2.setOutputKeyClass(Text.class);
//    job2.setOutputValueClass(IntWritable.class);
//
//    FileInputFormat.setInputPaths(job2, new Path("b"));
//    FileOutputFormat.setOutputPath(job2, new Path(args[1]));
//
//    job2.submit();
//    job2.waitForCompletion(true);
	}
}
