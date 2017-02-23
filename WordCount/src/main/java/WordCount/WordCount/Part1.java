package WordCount.WordCount;


import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

public class Part1 {
	public BufferedReader brTest = null;
	public String text = "";
	
	
	public ArrayList<String> readPositive(String address){
		//String[] eachrow = null;
		int checkRow = 0;
		ArrayList<String> positiveWords = new ArrayList<String>(); 
		
		try{
			brTest = new BufferedReader(new FileReader(address));
//			text = brTest.readLine();
//			System.out.println(text.charAt(0));
//			text = brTest.readLine();
//			System.out.println(text);
			while((text != null) ){
				text = brTest.readLine();	
				
				if ((text!=null)){
				//if(text.charAt(0) != ';'){	
					//System.out.println(text);
					positiveWords.add(text);
					
					checkRow++;
				}
			}
		    brTest.close();
		}
		catch(Exception e){
			System.out.println("could not find file");
		}
		
		//check part
		if(positiveWords.size() != checkRow){
			System.out.println("error");
		}
		else{
			System.out.println("load positive file correctly");
		}
		//check end
		ArrayList<String> result = new ArrayList<String>();
		for(int i = 0; i < positiveWords.size(); i++){
			if(positiveWords.get(i).length()>0){
				if(positiveWords.get(i).charAt(0) != ';'){
					result.add(positiveWords.get(i));
				}			
			}
		}
		return result;
	}
	public static void main(String[] args){
		Part1 p = new Part1();
		String address = "F:\\cs6350 big data\\assignment1b\\positive-words.txt";
		ArrayList<String> result = new ArrayList<String>();
		result = p.readPositive(address);
		for(int i = 0 ; i < result.size(); i++){
			System.out.println(result.get(i));
		}
	}
}
