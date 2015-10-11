package my;
import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

   public class StubMapper extends Mapper<Object, Text, Text, Text> {
	   
  @Override
  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    /*
     * TODO implement
     */
	  
	//Input of e.g --> ACGT<Tab>User1
	  String[] words = value.toString().split("[ \t]+");
	 
	  String User = words[0];
	  String DNA = words[1];
	  String Reverse_DNA="";
	  
	// Generating Reverse_DNA
	  		
	  for(int i=DNA.length()-1;i>=0 ; i=i-1)
			{Reverse_DNA = Reverse_DNA + DNA.substring(i, i+1);}
	  
	// Using the smaller Of(DNA, Reverse_DNA) and key and User as Value
		
	  if (DNA.compareTo(Reverse_DNA)<0)
		{context.write(new Text(DNA), new Text(User));}
	  else
		{context.write(new Text(Reverse_DNA), new Text(User));}
 
  }
}

