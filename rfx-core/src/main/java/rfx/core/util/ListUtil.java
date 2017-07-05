package rfx.core.util;

import java.util.ArrayList;
import java.util.List;

import rfx.core.util.StringPool;


public class ListUtil {

	public static List<String> toList(String[] toks){
		List<String> list = new ArrayList<String>(toks.length);
		for (String tok : toks) {
			if( ! StringPool.BLANK.equals(tok) )				
				list.add(tok);
		}
		return list;
	}
}
