package rfx.leveldb;

import org.iq80.leveldb.*;

import static org.iq80.leveldb.impl.Iq80DBFactory.*;

import java.io.*;

public class TestLevelDB {

	public static void main(String[] args) throws IOException {
		Options options = new Options();
		options.createIfMissing(true);
		DB db = factory.open(new File("data/leveldb/test"), options);
		try {
			//db.put(bytes("Tampa"), bytes("rocks"));
			String value = asString(db.get(bytes("Tampa")));
			System.out.println(value);
		} finally {
		  // Make sure you close the db to shutdown the 
		  // database and avoid resource leaks.
		  db.close();
		}
	}
	
}
