import java.util.*;
import java.io.*;

class dsuNormal{

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
 
   public static void main (String [] args)  throws IOException, InterruptedException{
     long startTime = System.nanoTime();
     DSU dsu = new DSU (10000);
     BufferedReader br = new BufferedReader(new FileReader("edgeData.txt"));
     String strLine = null;
     while((strLine = br.readLine()) !=null){
       StringTokenizer tokenizer = new StringTokenizer(strLine);
           if(tokenizer.hasMoreTokens()){
             int u = Integer.parseInt(tokenizer.nextToken());
             if(tokenizer.hasMoreTokens()){
               int v = Integer.parseInt(tokenizer.nextToken());
               dsu.combine(u,v);
             }
           }
     }
     
     HashMap<Integer,List<Integer>> map = new HashMap<>();
    
      for(int i = 0; i < dsu.parent.length; i++) {
    	  int u = dsu.parent[i];
    	  List<Integer> list = map.getOrDefault(u, new ArrayList<>());
    	  list.add(i);
    	  map.put(u, list);
      }
    
      for(int u : map.keySet()) {
        System.out.println(u + " " + map.get(u));
      }
     
     
     
     
     
     long endTime = System.nanoTime();
     System.out.println(endTime - startTime);
   
   }
 
 }