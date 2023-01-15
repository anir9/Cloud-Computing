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

public class dsu_mapreduce {
	
	static DSU dsu;
	
	public static class DSU{
		int [] size;
		int [] parent;
		
		public DSU (int n) {
			this.size = new int[n];
			this.parent = new int[n];
			Arrays.fill(this.size, 1);
			for(int i = 0; i < n; i++) {
				this.parent[i] = i;
			}
			
		}
		
		int find(int u) {
			if(this.parent[u] == u) return u;
			return this.parent[u] = find(this.parent[u]);
		}
		
		void combine (int u, int v) {
			u = find(u);
			v = find(v);
			
			if(u==v) return;
			
			if(this.size[u] > this.size[v]) {
				this.size[u] += this.size[v];
				this.parent[v] = u;
			}
			else {
				this.size[v] += this.size[u];
				this.parent[u] = v;
			}
			
		}
		
	}

	public class Node {
		String key;
		int value;

		public Node(String key, int value) {
			this.key = key;
			this.value = value;
		}
	}

	public static class Map extends Mapper<LongWritable, Text, LongWritable, LongWritable> {

        public void map(LongWritable key, Text edgeData, Context context) 
        throws IOException, InterruptedException{
          String line = edgeData.toString();
          
          /*
          try{
            StringTokenizer tokenizer = new StringTokenizer(line);
            long u = Long.parseLong(tokenizer.nextToken());
            long v = Long.parseLong(tokenizer.nextToken());
          	context.write(new LongWritable(u), new LongWritable(v));
           } catch(Exception e){}
           
           */
           
           StringTokenizer tokenizer = new StringTokenizer(line);
           if(tokenizer.hasMoreTokens()){
             long u = Long.parseLong(tokenizer.nextToken());
             if(tokenizer.hasMoreTokens()){
               long v = Long.parseLong(tokenizer.nextToken());
               context.write(new LongWritable(u), new LongWritable(v));
             }
           }
           
        }
    }


public static class Reduce extends Reducer<LongWritable, LongWritable, LongWritable, Text> {
    //private IntWritable findegree = new IntWritable();
    
    DSU dsu;
    public void setup (Context context) throws IOException, InterruptedException{
      this.dsu = new DSU(10000); 
    }

    public void reduce(LongWritable key, Iterable<LongWritable> valuesList, Context context) throws IOException, InterruptedException{
        //Configuration conf = context.getConfiguration();
        //DSU dsu = conf.get("dsu");
        int u = (int)key.get();
        Iterator<LongWritable> values = valuesList.iterator();
        while(values.hasNext()) {
        	int v = (int)values.next().get();
        	this.dsu.combine(u, v);
          //context.write(new LongWritable((long)(u)), new Text(v + ""));
        }
    }
    
    public void cleanup (Context context) throws IOException, InterruptedException{
      HashMap<Integer,List<Integer>> map = new HashMap<>();
    
      for(int i = 0; i < this.dsu.parent.length; i++) {
    	  int u = this.dsu.parent[i];
    	  List<Integer> list = map.getOrDefault(u, new ArrayList<>());
    	  list.add(i);
    	  map.put(u, list);
      }
    
      for(int u : map.keySet()) {
        context.write(new LongWritable((long)(u)), new Text(map.get(u).toString()));
      }
    }
}

public static void main(String[] args) throws Exception {
    //dsu = new DSU (10000);
	
	  Configuration conf = new Configuration();
    //conf.set("dsu", dsu);
    String temp_path = "temp";
    
    conf.setLong("mapreduce.input.fileinputformat.split.maxsize",140000);
    conf.setLong("mapreduce.input.fileinputformat.split.minsize",130000);

    Job job = Job.getInstance(conf);
    job.setJarByClass(dsu_mapreduce.class);
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);
    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(LongWritable.class);
    job.setNumReduceTasks(8);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    long startTime = System.nanoTime();
    job.submit();
    job.waitForCompletion(true);
    long endTime = System.nanoTime();
    System.out.println(endTime - startTime);
    
    
    
    
    

  }
}
















    
