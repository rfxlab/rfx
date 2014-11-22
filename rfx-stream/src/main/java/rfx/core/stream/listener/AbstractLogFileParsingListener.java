package rfx.core.stream.listener;

import java.io.Serializable;

public abstract class AbstractLogFileParsingListener implements Serializable{

	private static final long serialVersionUID = 4173912864582477042L;
	int lineCount;
	boolean isReading;
	
	public AbstractLogFileParsingListener(){
		lineCount = 0;
		isReading = false;
	}	
	
	
	public abstract void parsingLine(String line);
	
	public AbstractLogFileParsingListener increaseLineCount(){
		lineCount++;
		return this;
	}
	
	public int getLineCount() {
		return lineCount;
	}
	
	public void startReading(){
		isReading = true;
	}
	public void parsingDone() {
		isReading = false;		
	}
	public boolean isReading() {
		return isReading;
	}
}