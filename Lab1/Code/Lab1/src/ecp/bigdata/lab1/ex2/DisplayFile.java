package ecp.bigdata.lab1.ex2;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.FileSystem;


public class DisplayFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("TreeDataSet/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				String[] col = line.split(";");
				if (!col[5].equals("") & !col[6].equals("")){
					System.out.println("année : "+col[5]+", hauteur : "+col[6]);
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