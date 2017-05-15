package ecp.bigdata.lab1.ex2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompactFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("isd-history.txt");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			int lineCounter = 0;
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				
				lineCounter += 1;
				
				if (lineCounter > 22){
					String USAF = line.substring(0, 6);
					String Name = line.substring(13, 42);
					String FIP = line.substring(43, 45);
					String Alt = line.substring(74, 81);
					
					System.out.println(USAF+"; "+Name+"; "+FIP+"; "+Alt);
				}
				
				
				
				// go to the next line
				line = br.readLine();
			}
		}

		finally{
			
			//close the file
			inStream.close();
			fs.close();
		}
	}
}

