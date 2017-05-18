package ecp.bigdata.lab1.ex4.tfidf;

import java.io.*;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;


public class SortResults {
	
	public static <K, V extends Comparable<? super V>> Map<K, V> sortByValue( Map<K, V> map ){
		
		
		
		List<Map.Entry<K, V>> list = new LinkedList<Map.Entry<K, V>>( map.entrySet() );
		
		Collections.sort( list, new Comparator<Map.Entry<K, V>>() {
			public int compare( Map.Entry<K, V> o1, Map.Entry<K, V> o2 ) {
				return ((o2.getValue())).compareTo( o1.getValue() );
			}
		} );

		Map<K, V> result = new LinkedHashMap<K, V>();
		for (Map.Entry<K, V> entry : list) {
			result.put( entry.getKey(), entry.getValue() );
		}
		return result;
	}

	public static void main(String[] args) throws IOException {
		
		if (args.length != 1) {
            System.out.println("Usage: [input]");
            System.exit(-1);
        }
		
		Path filename = new Path(args[0]);
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		Map<String, Double> unsorted = new HashMap<String, Double>();
		

		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				String[] wdt = line.split(";");
				String word_doc = wdt[0] + ", " + wdt[1];
				double tfidf = Double.parseDouble(wdt[2]);
				
				unsorted.put(word_doc, tfidf);
				
				line = br.readLine();
			}
			
			Map<String, Double> sorted = SortResults.sortByValue( unsorted);
			
			Set<Entry<String, Double>> setMap = sorted.entrySet();
			Iterator<Entry<String, Double>> it = setMap.iterator();
			
			
			
			for(int i = 0; i <20; i++){
				Entry<String, Double> e = it.next();
				System.out.println(e.getKey()+ " : " + e.getValue());
			}
		}

		finally{
			//close the file
			inStream.close();
			fs.close();
		}
	}
}