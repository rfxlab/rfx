package rfx.core.stream.emitters;

import java.util.Queue;

import rfx.core.model.CallbackResult;

/**
 * @author trieunt
 *
 */
public class DataFileEmitter extends DataEmitter {
	
	Queue<String> dataFiles;
	
	public DataFileEmitter(Queue<String> dataFiles) {
		super();
		this.dataFiles = dataFiles;
	}
	
	public void addFile(String file){
		dataFiles.add(file);
	}
	
	@Override
	public CallbackResult<String> call() {		
		String path = dataFiles.poll();
		if(path != null){
			return new CallbackResult<String>(path);
		}
		return null;
	}

}
