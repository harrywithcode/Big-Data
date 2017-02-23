package ASS01B;

public class POSUtil {

	public static final String PAL = "Palindrome";
	public static final int LEN_LIMIT = 5;	
	
	public boolean isPalindrome(String s, int low, int high)
	{
		while (low < high)
		{
			if (s.charAt(low++) != s.charAt(high--))
				return false;
		}
		
		return true;
	}
}
