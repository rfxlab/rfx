package rfx.core.util;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.UUID;

import rfx.core.model.Callback;
import rfx.core.model.CallbackResult;


public class Utils {

    public static Object newInstance(String klass) {
        try {
            Class c = Class.forName(klass);
            return c.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static byte[] serialize(Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    public static Object deserialize(byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch(IOException ioe) {
            throw new RuntimeException(ioe);
        } catch(ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static <T> String join(Iterable<T> coll, String sep) {
        Iterator<T> it = coll.iterator();
        String ret = "";
        while(it.hasNext()) {
            ret = ret + it.next();
            if(it.hasNext()) {
                ret = ret + sep;
            }
        }
        return ret;
    }

    public static void foreverLoop() {
    	foreverLoop(1000, new Callback<Boolean>() {			
			@Override
			public CallbackResult<Boolean> call() {
				return new CallbackResult<Boolean>(false);
			}
		});
    }
    
    public static void foreverLoop(long sleepMillis, Callback<Boolean> exitCallback) {
    	while (true) {
    		sleep(sleepMillis);
    		if(exitCallback == null){
    			break;
    		} else {
    			CallbackResult<Boolean> rs = exitCallback.call();
    			if(rs == null){
    				break;
    			} else if(rs.getResult()){
    				break;	
    			}    			
    		} 
		}
    }

    public static void sleep(long millis) {
        try {
            Time.sleep(millis);
        } catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    
    public static void sleep(long millis, long timeout, Callback<Boolean> sleepCondition) {
    	while (sleepCondition.call().getResult() && timeout > 0) {
			Utils.sleep(millis);
			timeout -= millis;
			System.out.println(timeout);
		}
    }
    
    public static List<URL> findResources(String name) {
        try {
            Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
            List<URL> ret = new ArrayList<URL>();
            while(resources.hasMoreElements()) {
                ret.add(resources.nextElement());
            }
            return ret;
        } catch(IOException e) {
            throw new RuntimeException(e);
        }
    }



    public static <S, T> T get(Map<S, T> m, S key, T def) {
        T ret = m.get(key);
        if(ret==null) {
            ret = def;
        }
        return ret;
    }
    
    public static List<Object> tuple(Object... values) {
        List<Object> ret = new ArrayList<Object>();
        for(Object v: values) {
            ret.add(v);
        }
        return ret;
    }

    public static void downloadFromMaster(Map conf, String file, String localFile) throws Exception {
    	//FIXME
        //NimbusClient client = NimbusClient.getConfiguredClient(conf);
//        String id = client.getClient().beginFileDownload(file);
//        WritableByteChannel out = Channels.newChannel(new FileOutputStream(localFile));
//        while(true) {
//            ByteBuffer chunk = client.getClient().downloadChunk(id);
//            int written = out.write(chunk);
//            if(written==0) break;
//        }
//        out.close();
    } 
    
    
    public static boolean isSystemId(String id) {
        return id.startsWith("__");
    }
        
    public static <K, V> Map<V, K> reverseMap(Map<K, V> map) {
        Map<V, K> ret = new HashMap<V, K>();
        for(K key: map.keySet()) {
            ret.put(map.get(key), key);
        }
        return ret;
    }
    

    
    public static Integer getInt(Object o) {
        if(o instanceof Long) {
            return ((Long) o ).intValue();
        } else if (o instanceof Integer) {
            return (Integer) o;
        } else if (o instanceof Short) {
            return ((Short) o).intValue();
        } else {
            throw new IllegalArgumentException("Don't know how to convert " + o + " + to int");
        }
    }
    
    public static long secureRandomLong() {
        return UUID.randomUUID().getLeastSignificantBits();
    }
    
	public static final int randomActorId(int max) {
		int min = 0;		
		return randomNumber(min, max);
	}
	
	public static final int randomNumber(int min, int max) {		
		int num = min + (int) (Math.random() * ((max - min) + 1));
		return num;
	}

    
    public static TreeMap<Integer, Integer> integerDivided(int sum, int numPieces) {
        int base = sum / numPieces;
        int numInc = sum % numPieces;
        int numBases = numPieces - numInc;
        TreeMap<Integer, Integer> ret = new TreeMap<Integer, Integer>();
        ret.put(base, numBases);
        if(numInc!=0) {
            ret.put(base+1, numInc);
        }
        return ret;
    }

    public static byte[] toByteArray(ByteBuffer buffer) {
        byte[] ret = new byte[buffer.remaining()];
        buffer.get(ret, 0, ret.length);
        return ret;
    }
    
    public static void exitSystemAfterTimeout(final long delay){
    	new Timer().schedule(new TimerTask() {			
			@Override
			public void run() {
				System.exit(1);
			}
		}, delay);
    }
}
