package rfx.sample.income.parsing;

import java.io.Serializable;

import com.google.gson.Gson;

public class Person implements Serializable{
	private static final long serialVersionUID = -8917032934585345178L;
	int id;
	String education;
	String occupation;
	boolean incomeLess50k = true;
	boolean queried = false;
	String rawData;
	
	public Person(int id, String education, String occupation, boolean incomeLess50k) {		
		this.id = id;
		this.education = education;
		this.occupation = occupation;
		this.incomeLess50k = incomeLess50k;
	}

	public String getRawData() {
		return rawData;
	}
	public void setRawData(String rawData) {
		this.rawData = rawData;
	}
	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public String getEducation() {
		return education;
	}

	public void setEducation(String education) {
		this.education = education;
	}

	public boolean isIncomeLess50k() {
		return incomeLess50k;
	}

	public void setIncomeLess50k(boolean incomeLess50k) {
		this.incomeLess50k = incomeLess50k;
	}
	
	public String getOccupation() {
		return occupation;
	}

	public void setOccupation(String occupation) {
		this.occupation = occupation;
	}

	public boolean isQueried() {
		return queried;
	}

	public void setQueried(boolean queried) {
		this.queried = queried;
	}

	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
