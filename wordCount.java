import java.io.IOException;
import java.util.*;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


        
public class wordCount{

    
 public static class Map extends Mapper<LongWritable, Text, Text, Text> {
    
        
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

      Text count = new Text();
	  Text word = new Text();

	 String term ="";
      	 HashMap<String, Integer> wordcount = new HashMap<String, Integer>();

        String line = value.toString();
        StringTokenizer itr = new StringTokenizer(line);

	if (itr.hasMoreTokens()) 
	{
	  itr.nextToken(); 
		
		while (itr.hasMoreTokens()) 
		{
		    
		      term = itr.nextToken(); 
		      if (wordcount.containsKey(term)) 
			  wordcount.put(term,wordcount.get(term)+1); 
		       else 
			  wordcount.put(term,1);
		}
	  
		for (String s : wordcount.keySet()) {
		      word.set(s); 
		      count.set(1+" "+wordcount.get(s));

		      context.write(word, count); 
		}
	}
    }
 } 
  
     
 public static class Reduce extends Reducer<Text, Text, Text, Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) 
      throws IOException, InterruptedException {
       
	int num_docs=0;
	int word_count=0;
	
        for (Text val : values) 
       {
	    String []arr=val.toString().split(" ");
	    num_docs=num_docs+Integer.parseInt(arr[0]);
	    word_count=word_count+Integer.parseInt(arr[1]);
        }
        
	Text t_key = new Text(); 
	Text t_num_wc= new Text();
	t_key.set(key); 
	t_num_wc.set(num_docs+" "+word_count);
	context.write(t_key, t_num_wc);

    }
 }
        
 public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
        
    Job job = new Job(conf,"wordcount");
	job.setJarByClass(wordCount.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);   
    job.setMapperClass(Map.class);
    job.setReducerClass(Reduce.class);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.waitForCompletion(true);
 }
        
}


