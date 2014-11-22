package rfx.core.stream.util;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import rfx.core.util.HttpClientUtil;
import rfx.core.util.StringPool;
import twitter4j.JSONArray;
import twitter4j.JSONException;
import twitter4j.JSONObject;




public class SocialAnalyticsUtil {
	
	public static String readData(String url){
		StringBuilder data = new StringBuilder();
		try {
		    BufferedReader in = new BufferedReader(new InputStreamReader(new URL(url).openStream()));
		    
		    String line;
		    while ((line = in.readLine()) != null) {
		    	data.append(line);
		    }
		    in.close();
		}
		catch (MalformedURLException e) {
			System.out.println("Malformed URL: " + e.getMessage());
		}
		catch (IOException e) {
			System.out.println("I/O Error: " + e.getMessage());
		}
		return data.toString();
	}
	
	public static JSONArray getFacebookLinkData(String url){
		JSONArray obj = null;
		try {
			url = URLEncoder.encode(url, StringPool.UTF_8);
			String baseUrl = "http://api.facebook.com/method/fql.query?query=SELECT+via_id%2C+link_id+FROM+link+WHERE+url%3D%22"+url+"%22&format=json";
			System.out.println(baseUrl);
			String json = readData(baseUrl);
			System.out.println(json);
			obj = new JSONArray(json);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return obj;
	}

	public static JSONArray getLinkFacebookStats(String url){
		JSONArray obj = null;
		try {
			url = URLEncoder.encode(url, StringPool.UTF_8);
			String baseUrl = "http://api.facebook.com/method/fql.query?query=SELECT+url%2C+normalized_url%2C+share_count%2C+like_count%2C+comment_count%2C+total_count%2C+commentsbox_count%2C+comments_fbid%2C+click_count+FROM+link_stat+WHERE+url%3D%22"+url+"%22&format=json";
			System.out.println(baseUrl);
			String json = readData(baseUrl);
			System.out.println(json);
			obj = new JSONArray(json);
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	public static Elements getLinks(String url){
		try {
			String html = HttpClientUtil.executeGet(url);
			Document doc = Jsoup.parse(html);			
			return doc.select("a[href]");			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new Elements(0);		
	}
	
	public static int getVnExpressLikeCount(String url){
		try {
			String html = readData(url);
			Document doc = Jsoup.parse(html);			
			Elements spans = doc.select("a[total-like]");
			if(spans.size()>0){
				String text = spans.get(0).attr("total-like");
				return Integer.parseInt(text);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;		
	}
	
	public static int getTotalCountFacebookStats(String url){
		JSONArray array = getLinkFacebookStats(url);
		System.out.println(array);
		try {
			for (int i = 0; i < array.length(); i++) {
				JSONObject obj = array.getJSONObject(i);
				return obj.getInt("total_count");
			}
		} catch (JSONException e) {			
			e.printStackTrace();
		}
		return 0;
	}
	
	public static int getFacebookLikeCount(String url){
		JSONArray array = getLinkFacebookStats(url);
		System.out.println(array);
		try {
			for (int i = 0; i < array.length(); i++) {
				JSONObject obj = array.getJSONObject(i);
				return obj.getInt("like_count");
			}
		} catch (JSONException e) {			
			e.printStackTrace();
		}
		return 0;
	}
	
	public static int getFacebookShareCount(String url){
		JSONArray array = getLinkFacebookStats(url);
		System.out.println(array);
		try {
			for (int i = 0; i < array.length(); i++) {
				JSONObject obj = array.getJSONObject(i);
				return obj.getInt("share_count");
			}
		} catch (JSONException e) {			
			e.printStackTrace();
		}
		return 0;
	}
	
	
	public static JSONObject getLinkInfoOnFacebook(String url){
		String baseUrl = "http://graph.facebook.com/?ids=";
		JSONObject obj = null;
		try {
			baseUrl = baseUrl +  URLEncoder.encode(url, StringPool.UTF_8);
			obj = new JSONObject(HttpClientUtil.executeGet(baseUrl));
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		return obj;
	}
	
	public static int getTweetCount(String url){
		String baseUrl = "http://urls.api.twitter.com/1/urls/count.json?url=";
		JSONObject obj = null;
		try {
			baseUrl = baseUrl +  URLEncoder.encode(url, StringPool.UTF_8);
			obj = new JSONObject(readData(baseUrl));
			return obj.getInt("count");
		} catch (Exception e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	public static int getTotalUserLikeUrl(String url){
		int c = getTotalCountFacebookStats(url);
		c += getTweetCount(url);
		c += getVnExpressLikeCount(url);
		return c;
	}
	
	public static  void main(String[] args){
//		String url = "http://giaitri.vnexpress.net/tin-tuc/nhac/lang-nhac/nhac-si-pham-duy-qua-doi-2419616.html";
//		int c = getTotalCountFacebookStats("http://vnexpress.net/gl/xa-hoi/2013/01/duong-hoa-nguyen-hue-van-hoa-tet-cua-sai-gon/");
//		System.out.println(getTweetCount("http://vnexpress.net/tin-tuc/thoi-su/100-000-dong-khong-mua-noi-trai-cam-xa-doai-2943569.html"));
//		testExtractTwitterText();
//		System.out.println(getFacebookLinkData("http://vnexpress.net/tin-tuc/thoi-su/nguoi-sai-gon-phong-sinh-ngay-ong-cong-ong-tao-2944073.html"));
	
		//Facebook facebook = new FacebookFactory().getInstance();
		
		String testurl = "http://vnexpress.net/gl/kinh-doanh/quoc-te/2012/12/10-quang-cao-hay-nhat-nam-2012/";
		//System.out.println(getLinkInfoOnFacebook(testurl));
                	
		System.out.println(getTweetCount(testurl));
		
		
	}
	
	
	
	static void testApi(){
		String domain = "vnexpress.net";
		String testurl = "http://vnexpress.net/gl/kinh-doanh/quoc-te/2012/12/10-quang-cao-hay-nhat-nam-2012/";
		System.out.println(getTweetCount(testurl));
		//System.out.println(getLinkInfoOnFacebook(testurl));
		System.out.println(getTotalCountFacebookStats(testurl));
		System.out.println(getVnExpressLikeCount(testurl));
		System.out.println(getTotalUserLikeUrl(testurl));
		Elements nodes = getLinks(testurl);
		for (Element element : nodes) {
			String href = element.attr("href");
			if(href != null){
				href = href.trim();
				boolean ok = false;
				if( ! href.startsWith("javascript")  && ! href.startsWith("#") && href.length()>1 ){
					if(href.startsWith("http")&&href.contains(domain)){
						ok = true;
					} else if(href.startsWith("/")){
						ok = true;
						href = "http://" + domain + href;
					}
					if(ok){
						System.out.println(href);	
					}					
				}
				
			}
		}
		
	}
	
}
