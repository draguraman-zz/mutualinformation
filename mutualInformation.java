import java.io.*;
import java.util.*;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

//Brute force mutual Information computing

public class mutualInformation {
	//word associations for all the words that have occurred at least 3 times in the whole AP data set, but with a document frequency lower than 30%.Somewehere
	public static HashMap<String,Integer> eligibleWords = new HashMap<String,Integer>(); 
		
	public static int Num_Docs = 0; //total no of docs in the collection


	public static void main(String[] args) throws IOException
	{
		//setting up Confs and Hadoop access
		Configuration config = new Configuration();
		FileSystem file_sys = FileSystem.get(config);
		FSDataInputStream data,wordinput,wordpairinput;
		FSDataOutputStream outputmutualinfopairs;

		if(args.length != 4)
		{
			System.err.println("Invalid Args"+" usage : hadoop jar <path to jar file> mutualInformation(className) <src-datafile> <wordcountfile> <wordpaircountfile> <outputfile>");
			System.exit(1);
		}
		else
			System.out.println("no of arguments is "+args.length);
		
		try {
			//open all file streams
				data = file_sys.open(new Path(args[0]));
				wordinput = file_sys.open(new Path(args[1])); 
				wordpairinput = file_sys.open(new Path(args[2])); 
				outputmutualinfopairs = file_sys.create(new Path(args[3])); //
		
				//# of docs equivalent to number of line in apsrc.txt
				BufferedReader buffer = new BufferedReader(new InputStreamReader(data));
                String line = null;
               
                while((line = buffer.readLine())!= null)
                {
                	Num_Docs++;
                }
		       
                buffer.close();
                
                //read the word counts file
                double document_parameter;
                buffer = new BufferedReader(new InputStreamReader(wordinput));
        		line = null;
        		int word_count, document_count;
        		
        		while((line = buffer.readLine())!= null)
        		
        		{
        			StringTokenizer st = new StringTokenizer(line);
        			
        			int count = st.countTokens();
        			String []contents = new String[count];//line.split(" ");
        			for(int k =0 ;k<count;k++)
        			{
        				if(st.hasMoreTokens())
        				{
        					contents[k] = st.nextToken();
        				}
        			}
        			
        			word_count=Integer.parseInt(contents[2]);
        			document_count=Integer.parseInt(contents[1]);
        			
        			if(word_count>3)
        			{
        				document_parameter=(double)document_count/(double)Num_Docs;
        					if(document_parameter < 0.3)
        						eligibleWords.put(contents[0],document_count);	
        			}
        		}
        		buffer.close();
        		
        		//now read the word pairs file	
        		buffer = new BufferedReader(new InputStreamReader(wordpairinput));
        		line = null;
        		String word_1, word_2;
        		
        		while((line=buffer.readLine())!=null)
        		{
        			line.replaceAll("	"," ");
        			StringTokenizer st = new StringTokenizer(line);
        			String [] contents = new String[st.countTokens()];
        			int k=0;
        			
        			while(st.hasMoreTokens())
        			{
        				contents[k]=st.nextToken();
        				k++;
        			}
        			
        			word_1=contents[0];
        			word_2=contents[1];
        			
        			if(eligibleWords.containsKey(word_1) && eligibleWords.containsKey(word_2))
        			{
        				//compute mutual information
        				int N_a=eligibleWords.get(word_1);
        				int N_b=eligibleWords.get(word_2);
        				int N_ab=Integer.parseInt(contents[2]);
        				System.out.println("Starting Computation for each pair");
        				System.out.println("Num_a = "+N_a+" Num_b = "+N_b+" Num_ab = "+N_ab);
        				
        				//Write out to the MutualInformation.result file
    					double xy = (N_ab + 0.25)/(Num_Docs+1);
    					double x = (N_a+0.5)/(Num_Docs+1);
    					double y = (N_b+0.5)/(Num_Docs+1);
    					double mutualInfo = xy*Math.log(xy/(x*y));
    					outputmutualinfopairs.writeUTF("("+word_1+","+word_2+")	:"+mutualInfo+"\n");
    					    					
        			}
        			
        			
        			
        		}
        	System.out.println("Computation Complete");
    
			buffer.close();
			outputmutualinfopairs.close();
}// end try		
		
		catch(IOException io)
		{
			System.out.println("Where are the input Files?? ");
			io.printStackTrace();
			System.exit(1);
		}
		System.out.println("Program Ends"); // TODO :would be cool if i could do this also inside hadoop
	}//end main
}
