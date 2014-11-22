package server.http.model;

import com.google.gson.Gson;


public class ClickBeaconData {
		
	String redirectUrl;
					
	public ClickBeaconData(String redirectUrl, String beacon) {
		super();
		this.redirectUrl = redirectUrl;
					
	}
	
	public String getRedirectUrl() {
		return redirectUrl;
	}
	public void setRedirectUrl(String redirectUrl) {
		this.redirectUrl = redirectUrl;
	}	
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}		
}

