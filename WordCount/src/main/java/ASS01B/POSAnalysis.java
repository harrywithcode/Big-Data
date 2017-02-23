
package ASS01B;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;

public class POSAnalysis {

	private static final Logger LOG = Logger.getLogger(POSAnalysis.class.getName());
  
	
	  public static void main(String[] args) throws Exception {

		// Retrieve job tracker and yarn resource address from command line
	    String mapredJobTracker = args[2].trim();
	    String yarnResourceManagerAddress= args[3].trim();
		  
		  
	    Configuration conf = new Configuration();
	    // Local environment
	    conf.set("mapred.job.tracker", mapredJobTracker);
	    conf.set("yarn.resourcemanager.address", yarnResourceManagerAddress);
	    conf.set("mapreduce.framework.name", "yarn");
	    
	    // Passing argument for positive and negative
	    //================================================
		String posFilePath = args[4].trim();
	    conf.set("POSFilePath", posFilePath);
	    System.out.println("==================================");
	    System.out.println("POSFilePath: " + posFilePath );	    
	    System.out.println("==================================");	    
	    
	    Job job = Job.getInstance(conf, "top N count");
	    job.setJarByClass(POSAnalysis.class);	    
	    job.setMapperClass(POSMapper.class);
	    job.setReducerClass(POSReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);	    
	    //job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    

	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

	  
  public static class POSMapper
       extends Mapper<Object, Text, Text ,Text>{

    private String tokens = "[_|$@#<>\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
    

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException 
    {
    	
      // Obtain command line parameter	
      Configuration conf = context.getConfiguration();
      String posFilePath = conf.get("POSFilePath");
	  Map<String, List<String>> posDic = getSentimentSet(posFilePath);      
                   	
    // Clean up file  
      String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
    // Break words
      StringTokenizer itr = new StringTokenizer(cleanLine);
       
     emitPOS(posDic, itr, context); 
     //emitOnce(itr,context);

    }
    
    private void emitPOS(Map<String, List<String>> posDic, StringTokenizer itr, Context context) throws IOException, InterruptedException
    {    	
        String token = null;
        // This class has palindrome function
        POSUtil posUtil = new POSUtil();
                      
        // lenStr as key to be emmited
        Text lenStr = new Text();
        
        // Iterate all input 
        while (itr.hasMoreTokens()) {
	      	token = itr.nextToken();
	      	int lenToken = token.length();
	      		      	
	      	// Handle word length is greater than 4
	      	if (token == null || token.length() < 1 || POSUtil.LEN_LIMIT > lenToken)
	      		continue;

	      	// Set to length
  			lenStr.set(String.valueOf(lenToken));	      	
	      	
	      	// Handle in the case of palindrome
	      	if (posUtil.isPalindrome(token, 0, lenToken-1))
	      	{	      		
      			context.write(lenStr, new Text(POSUtil.PAL));	      		
	      	}
	      	
	      	// Handle in case that POS dictionary has a word
	      	if (posDic.containsKey(token))
	      	{
	      		List<String> posRecord = posDic.get(token);
	      		// if one word has multiple POS, emit all POSs
	      		for (String pos : posRecord)
	      		{
	      			context.write(lenStr, new Text(pos));
	      		}
	      	}
        }
        
    }    

    
    private  Map<String, List<String>> getSentimentSet(String path) throws IOException 
    {
    	// define result
    	Map <String, List<String>> posDic = new HashMap<String, List<String>>();
    		
    	Path filePath = new Path(path);
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		
		String line = null;
		
		
		// The file has one word per line.
		while ((line = br.readLine()) != null)
		{
			String[] words = line.split("�");
			
			int lenWords = words.length;
			
			// Even though ASCII 215 is used, about 1223 lines are not accurately split
			if (lenWords != 2)
			{
				continue;
			}
			else 
			{
				String key = words[0];
				String value = words[1];
				
				if (posDic.containsKey(key))
				{
					List<String> posRecord = posDic.get(key);
					posRecord.add(value);
					posDic.put(key, posRecord);		
				}
				else
				{
					List<String> posRecord = new ArrayList<String>();
					posRecord.add(value);
					posDic.put(key, posRecord);								
				}				
			}						
		}			
    	return posDic;
    }
   
  }
    
			
  public static class POSReducer extends Reducer<Text,Text, Text, Text> 
  {
	  
    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException 
    {
      emitTitle(key, values, context);      
      emitContent(key, values, context);          
    }
    
    // Emit Count of words
    private void emitTitle(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
        context.write(new Text("Length"),key);    	    	
    }    
    
    // Emit Count of words
    private void emitContent(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException
    {
    	Map<String, Integer> posDist = new HashMap<String, Integer>();
        //String pos = values.toString();
    	
        int wordCount = 0;
        int palCount = 0;
        for (Text val : values) 
        {
	      	//LOG.info("Value in reducer" + val);        	
	      	// For counting words
        	wordCount++;
	        // POS distribution
	        String pos = val.toString();
	          
	        // Count palindrom
	        if (pos.equals(POSUtil.PAL))
	        {
	        	palCount++;
	        }
	        	
	        // Count POS  
	        if (posDist.containsKey(pos))
	        {
	        	posDist.put(pos, posDist.get(pos) + 1);
	        }
	        else
	        {
	        	posDist.put(pos, 1);
	        }
        }
       
        context.write(new Text("Count of words: "), new Text(String.valueOf(wordCount)));    	    	
        context.write(new Text("Distribution of POS: "), new Text(posDist.toString()));
        context.write(new Text("Number of palindroms: "), new Text(String.valueOf(palCount)));
        
  } // end of private
 } // end of reducer 
} // end of POSAnalysis