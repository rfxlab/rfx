package rfx.core.util;

import java.util.Random;
import java.util.UUID;

/**
 * @author trieu
 *
 */
public class RandomUtil {
	
	/**
	 * @param max
	 * @return random integer between 0 and given number
	 */
	public static int getRandom(int max) {
		return (int) (Math.random() * max);
	}

	/**
	 * @param maximum
	 * @param minimum
	 * @return random integer between minimum and maximum range
	 */
	public static int getRandomInteger(int maximum, int minimum) {
		return ((int) (Math.random() * (maximum - minimum))) + minimum;
	}
	
	public static final int randomNumber(int min, int max) {		
		int num = min + (int) (Math.random() * ((max - min) + 1));
		return num;
	}
	
	/**
	 * @param length
	 * @return random string with the specified length 
	 */
	public static String getRamdomString(int length){
		return new RandomString(length).nextString();
	}
	
    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }
    
    
	public static final int randomActorId(int max) {
		int min = 0;		
		return randomNumber(min, max);
	}
	



	public static class RandomString {
		private static final char[] symbols;

		static {
			StringBuilder tmp = new StringBuilder();			
			for (char ch = 'a'; ch <= 'z'; ++ch)
				tmp.append(ch);
			for (char ch = '0'; ch <= '9'; ++ch)
				tmp.append(ch);
			for (char ch = 'A'; ch <= 'Z'; ++ch)
				tmp.append(ch);			
			symbols = tmp.toString().toCharArray();
		}

		private final Random random = new Random();

		private final char[] buf;

		public RandomString(int length) {
			if (length < 1)
				throw new IllegalArgumentException("length < 1: " + length);
			buf = new char[length];
		}

		public String nextString() {
			for (int idx = 0; idx < buf.length; ++idx)
				buf[idx] = symbols[random.nextInt(symbols.length)];
			return new String(buf);
		}
	}
	
	public static void main(String[] args) {
		System.out.println(getRamdomString(20));
	}
}
