package ecp.bigdata.lab1.ex2;


import java.io.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;



public class CompterLigneFile {

	public static void main(String[] args) throws IOException {
		
		
		Path filename = new Path("TreeDataSet/arbres.csv");
		
		//Open the file
		Configuration conf = new Configuration();
		FileSystem fs = FileSystem.get(conf);
		FSDataInputStream inStream = fs.open(filename);
		
		int lineCounter = 0;
		
		try{
			
			InputStreamReader isr = new InputStreamReader(inStream);
			BufferedReader br = new BufferedReader(isr);
			
			// read line by line
			String line = br.readLine();
			while (line !=null){
				// Process of the current line
				lineCounter += 1;
				// go to the next line
				line = br.readLine();
			}
		}

		finally{
			System.out.println("Le nombre de lignes est :");
			System.out.println(lineCounter);
			//close the file
			inStream.close();
			fs.close();
		}
	}
}

