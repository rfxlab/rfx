package rfx.sample;




import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

import org.apache.storm.guava.base.Stopwatch;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;




/**
 * This topology demonstrates Storm's stream groupings and multilang capabilities.
 */
public class LogStatisticsTopology {

	final static SortedMap<String, UrlStatisticsRecord> urlStatisticsRecords = new TreeMap<String, UrlStatisticsRecord>();
	final static SortedMap<String, Integer> domainStatisticsRecords = new TreeMap<String, Integer>();
	final static Set<String> inValidLogTokenSet = new HashSet<String>();
	
	public static class UrlStatisticsRecord implements Comparable<UrlStatisticsRecord> , Serializable{
		private static final long serialVersionUID = 7085951463616038177L;
		int impressionCount = 0;
		int clickCount = 0;
		String url;
		String title;
		
		public UrlStatisticsRecord(String url) {
			super();
			this.url = url;			
		}		

		public String getTitle() {
			return title;
		}
		public void setTitle(String title) {
			this.title = title;
		}
		public int getImpressionCount() {
			return impressionCount;
		}		
		public int incrImpressCount(){
			return ++impressionCount;
		}
		public void setImpressionCount(int impressionCount) {
			this.impressionCount = impressionCount;
		}		
		public int getClickCount() {
			return clickCount;
		}
		public void setClickCount(int clickCount) {
			this.clickCount = clickCount;
		}
		public int incrClickCount(){
			return ++clickCount;
		}
		public String getUrl() {
			return url;
		}
		public void setUrl(String url) {
			this.url = url;
		}
		
		@Override
		public int compareTo(UrlStatisticsRecord o) {
			if(this.impressionCount > o.getImpressionCount()){
				return 1;
			} else if(this.impressionCount < o.getImpressionCount()){
				return -1;
			}
			return 0;
		}
				
		 @Override
		public String toString() {
			StringBuilder s = new StringBuilder(title);
			s.append(" : ").append(url);			
			s.append(" -->impressed: ").append(impressionCount);					
			return s.toString();
		}
	}
	
	public static class TokenizeLogRecord extends BaseRichBolt {
		private static final long serialVersionUID = 1L;
		OutputCollector _collector;
        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        @Override
        public void execute(Tuple tuple) {
        	String logEntryLine = tuple.getString(0);
        	//String[] words = sentence.split(" ");
        	AccessLogData data = new AccessLogData(logEntryLine);      	
    					
        	List<Object> values = new ArrayList<Object>(1);
        	values.add(data);
        	_collector.emit(values);
        	//System.out.println("SplitSentenceInJava: " + sentence);            
            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("accessLogData"));
        }
    }
	
	static String joinString(String[] toks, String delimiter){
		StringBuilder s = new StringBuilder();
		for (String tok : toks) {
			s.append(tok).append(delimiter);
		}
		return s.toString();
	}
	
	 public static class DomainAdsCountProcessor extends BaseRichBolt {		 
		private static final long serialVersionUID = 1L;
		OutputCollector _collector;		

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

		@Override
        public void execute(Tuple tuple) {            
			try {
				String url = tuple.getString(0);
				
	            //_collector.emit(new Values(domain, c));
	            
			} catch (Exception e) {				
				e.printStackTrace();
			}      
			_collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("domain", "ads_count"));
        }
              
    }
		
    
    public static class UrlLoadProcessor extends BaseRichBolt {
    	
		private static final long serialVersionUID = 1L;
		OutputCollector _collector;		

        @Override
        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

		@Override
		 public void execute(Tuple tuple) {             
			try {
				AccessLogData data = (AccessLogData)tuple.getValueByField("accessLogData");
				
				System.out.println(data.getRequest());
	            
			} catch (Exception e) {				
				e.printStackTrace();
			}
			_collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("url", "url_stats"));
        }
              
    }    
   
    
    public static void testSampleData(String logfile)  throws Exception  {
    	
        TopologyBuilder builder = new TopologyBuilder();
        LogReaderSpout logReaderSpout = new LogReaderSpout();
        builder.setSpout("spout",new LogReaderSpout() , 1);
        builder.setBolt("TokenizeLogRecord", new TokenizeLogRecord(), 10).shuffleGrouping("spout");        
        builder.setBolt("UrlLoadProcessor", new UrlLoadProcessor(),20).shuffleGrouping("TokenizeLogRecord");
//        builder.setBolt("DomainCountProcessor", new DomainAdsCountProcessor(),10).shuffleGrouping("UrlLoadProcessor");

        Config conf = new Config();
        //conf.setDebug(true);
        conf.put("logfile", logfile);
    
        conf.setMaxTaskParallelism(5);

        final LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("log-count", conf, builder.createTopology());
	
		try {
			while (logReaderSpout.hasNext()) {
				Thread.sleep(2000);				
			}
			System.out.println("### spout is done!");
			logReaderSpout.close();
		} catch (Exception e) { e.printStackTrace();	}
		//DONE, now print stats number
		
		int totalImpress = 0, totalClick = 0;
		Set<String> urls = urlStatisticsRecords.keySet();
        for (String url : urls) {
        	UrlStatisticsRecord record = urlStatisticsRecords.get(url);
        	totalImpress += record.getImpressionCount();
        	totalClick += record.getClickCount();
        	System.out.println("UrlStatisticsRecord: " + record);
		}
        Set<String> domains = domainStatisticsRecords.keySet();
        for (String domain : domains) {        	
        	System.out.println("domainStatisticsRecord: "+ domain +" "+domainStatisticsRecords.get(domain));
		}
        for (String token : inValidLogTokenSet) {        	
        	System.out.println("inValidLogToken: "+ token);
		}
        System.out.println("total_domain: " + domainStatisticsRecords.size());
        System.out.println("total_stats: " + urlStatisticsRecords.size());
        System.out.println("total_row: " + LogReaderSpout.rowCount);
        System.out.println("totalClick: " + totalClick);
        System.out.println("totalImpress: " + totalImpress);
        System.out.println("inValidLogTokenSet.size: " + inValidLogTokenSet.size());
        
        int totalProcessedTokens = totalClick + totalImpress + inValidLogTokenSet.size();
        System.out.println("---> totalProcessedTokens: " + totalProcessedTokens);
     
		cluster.shutdown();
		      
    }
    
    public static void main(String[] args) throws InterruptedException  {
    	final Stopwatch timer = Stopwatch.createStarted();
    	try {
    		//"data/eclick-log.txt"
			//testSampleData("data/eclick-log-10k.txt");
    		testSampleData("/home/trieu/data/nguyentantrieu.info-Aug-2014");
			//buildUserAnalyticsTopology();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	timer.stop();
        System.out.println("ElapsedTime(seconds): "+(timer.elapsed(TimeUnit.SECONDS)));
        Thread.sleep(1000);  
    }
    //ElapsedTime(seconds): 221 total_row: 9987
}
