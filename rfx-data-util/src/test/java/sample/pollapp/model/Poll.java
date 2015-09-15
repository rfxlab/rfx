package sample.pollapp.model;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.google.gson.Gson;
import com.google.gson.annotations.Expose;

public class Poll implements Comparable<Poll> {
	
	@Expose
	int id;
	@Expose
	String question;
	@Expose
	Date publishedDate;
	@Expose
	List<Choice> choices = new ArrayList<Choice>();
	@Expose
	int totalVotes;
	
	public int getId() {
		return id;
	}
	public void setId(int id) {
		this.id = id;
	}
	public String getQuestion() {
		return question;
	}
	public void setQuestion(String question) {
		this.question = question;
	}
	public Date getPublishedDate() {
		return publishedDate;
	}
	public void setPublishedDate(Date publishedDate) {
		this.publishedDate = publishedDate;
	}
	
	public List<Choice> getChoices() {
		return choices;
	}
	public void setChoices(List<Choice> choices) {
		if(choices != null){
			this.choices = choices;
			for (Choice choice : choices) {
				this.totalVotes += choice.getVotes();
			}
		}
	}
	public void addChoice(Choice choice){
		this.choices.add(choice);
		this.totalVotes += choice.getVotes();
	}
	
	@Override
	public String toString() {
		return new Gson().toJson(this);
	}
	
	@Override
	public int hashCode() {
		return id;
	}
	
	@Override
	public boolean equals(Object obj) {
		// TODO Auto-generated method stub
		return this.hashCode() == obj.hashCode();
	}	
	
	public int getTotalVotes() {
		return totalVotes;
	}
	@Override
	public int compareTo(Poll o) {
		if(this.getTotalVotes() < o.getTotalVotes()){
			return -1;
		} else if(this.getTotalVotes() > o.getTotalVotes()){
			return 1;
		}
		return 0;
	}
	
	
}
