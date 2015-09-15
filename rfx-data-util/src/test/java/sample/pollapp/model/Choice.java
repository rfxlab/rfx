package sample.pollapp.model;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

public class Choice {
	@Expose
	int id;
	@Expose
	int pollId;
	@Expose
	String text;
	@Expose
	int votes;
	
	public Choice(int id, int pollId, String text, int votes) {
		super();
		this.id = id;
		this.pollId = pollId;
		this.text = text;
		this.votes = votes;
	}
	
	public Choice() {
	}
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public int getPollId() {
		return pollId;
	}
	public void setPollId(int pollId) {
		this.pollId = pollId;
	}
	public String getText() {
		return text;
	}
	public void setText(String text) {
		this.text = text;
	}
	public int getVotes() {
		return votes;
	}
	public void setVotes(int votes) {
		this.votes = votes;
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
}
