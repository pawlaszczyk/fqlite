package fqlite.types;

public class TimeStamp {
	
	public Number raw;
	public String text;
	
	
	public TimeStamp(String text, Number raw){
		this.raw = raw;
		this.text = text;
	}

}
