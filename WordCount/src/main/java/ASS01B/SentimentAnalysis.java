
package ASS01B;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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

public class SentimentAnalysis {

/*
 hadoop jar WordCount.jar WordCount.WordCount.SenimentAnalysisLocal hdfs://hadoop-master:9000/user/parallels/input hdfs://hadoop-master:9000/user/parallels/ass01-b/part-a/output/answer  "hdfs://hadoop-master:54311 hadoop-master:8032 hdfs://hadoop-master:9000/user/parallels/resource/positive-words.txt hdfs://hadoop-master:9000/user/parallels/resource/negative.words.txt 
*/
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
		String positiveFilePath = args[4].trim();
		String negativeFilePath= args[5].trim();
	    conf.set("positvieFile", positiveFilePath);
	    conf.set("negativeFile", negativeFilePath);  
	    
	    Job job = Job.getInstance(conf, "top N count");
	    job.setJarByClass(SentimentAnalysis.class);	    
	    job.setMapperClass(SentimentMapper.class);
	    job.setReducerClass(SentimentWordReducer.class);
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(IntWritable.class);
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }

	  
  public static class SentimentMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    //private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private String tokens = "[_|$@#<>\\[\\]\\*/\\\\,;,.\\-:()?!\"']";
    

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
    	
      // Obtain command line parameter	
      Configuration conf = context.getConfiguration();
      
      String positiveFilePath = conf.get("positvieFile");
      String negativeFilePath = conf.get("negativeFile");
    	
    // Read resource file from HDFS and load words into positive and negative dictionary
      HashSet<String> positiveDic = getSentimentSet(positiveFilePath);
      HashSet<String> negativeDic = getSentimentSet(negativeFilePath);   
                  
    	
    // Clean up file  
      String cleanLine = value.toString().toLowerCase().replaceAll(tokens, " ");
    // Break words
      StringTokenizer itr = new StringTokenizer(cleanLine);
            
      emitOnce(positiveDic, negativeDic, itr,context);

    }
    
    private void emitOnce(HashSet<String> positiveDic, HashSet<String> negativeDic, StringTokenizer itr, Context context)
    throws IOException, InterruptedException
    {
        Text word = new Text();    	
        String token = null;
        
        int positiveCount = 0;
        int negativeCount = 0;      
        int othersCount = 0;      
              
        // count positive, negative and other words
        while (itr.hasMoreTokens()) {
      	token = itr.nextToken();
      	if (positiveDic.contains(token))
      		positiveCount++;
      	else if (negativeDic.contains(token))
      		negativeCount++;
      	else
      		othersCount++;    		
        }
        
        // Set to counts
        IntWritable positiveTotal = new IntWritable(positiveCount);
        IntWritable negativeTotal = new IntWritable(negativeCount);
        IntWritable othersTotal = new IntWritable(othersCount);
        
        // emit (K,V) such as K is positive and V is total positive counts
        word.set("Positive");
        context.write(word, positiveTotal);
        
        // emit (K,V) such as K is negative and V is total negative counts      
        word.set("Negative");
        context.write(word, negativeTotal);
        
        // emit (K,V) such as K is others and V is total other counts      
        word.set("Others");
        context.write(word, othersTotal);       	
    }
    
    private HashSet<String> getSentimentSet(String path) throws IOException 
    {
    	// define result
    	HashSet<String> sentimentSet = new HashSet<String>();
    	
    	Path filePath = new Path(path);
		Configuration conf = new Configuration();
		
		FileSystem fs = FileSystem.get(conf);		
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(filePath)));
		
		String word = null;
		
		// The file has one word per line.
		while ((word = br.readLine()) != null)
		{
			if (! sentimentSet.contains(word))
			{
				sentimentSet.add(word);
			}
		}		
    	return sentimentSet;
    }
  }

  public static class SentimentWordReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
	  
    private IntWritable result = new IntWritable();

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
    	    	
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }
      
      result.set(sum);
      context.write(key, result);      
    }

  }


}