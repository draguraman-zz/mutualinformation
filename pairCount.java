import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


        
public class pairCount{

//Mapper Class
 public static class Map extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    private final static IntWritable one = new IntWritable(1);
    
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    List<String> unique = new ArrayList<String>();
	String newword ="";
	int itr_i,itr_j;
    String line = value.toString();
    StringTokenizer itr = new StringTokenizer(line);
	if (itr.hasMoreTokens()) 
	{
	  itr.nextToken(); 
		
		while (itr.hasMoreTokens()) 
		{
		      newword = itr.nextToken(); 
		      if (!(unique.contains(newword))) 
			  unique.add(newword); 
		}
		Collections.sort(unique);
		for (itr_i=0;itr_i<unique.size()-1;itr_i++)
			for (itr_j=itr_i+1;itr_j<unique.size();itr_j++)
		{
		      Text text = new Text();
		      text.set(unique.get(itr_i)+ " " +unique.get(itr_j));
		      context.write(text, one); 
		}
	}
   }
 } 
  
 //Reducer class   
public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce(Text key, Iterable<IntWritable> values, Context context) 
      throws IOException, InterruptedException {
    	int num_docs = 0;
        for (IntWritable val : values) {
            num_docs=num_docs + Integer.parseInt(val.toString());
        }
        
        context.write(key, new IntWritable(num_docs)); 
    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();   
    Job job = new Job(conf, "paircount");
	job.setJarByClass(pairCount.class);
    job.setOutputValueClass(IntWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setReducerClass(Reduce.class); 
    job.setMapperClass(Map.class);    
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);   
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));  
    job.waitForCompletion(true);
 }
        
}


