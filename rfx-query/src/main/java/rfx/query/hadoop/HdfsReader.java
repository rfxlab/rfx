package rfx.query.hadoop;


public class HdfsReader {

//	public static void main(String[] args) {
//		try {
//			Configuration conf = new Configuration();
//		    conf.addResource(new Path("configs/hadoop/core-site.xml"));
//		    conf.addResource(new Path("configs/hadoop/hdfs-site.xml"));
//			String hdfsPath = "hdfs://localhost:54310/nguyentantrieu-info-access-logs/Nov-2013";
//			Path path = new Path(hdfsPath);
//			FileSystem fs = FileSystem.get(conf);
//			
//			FileStatus[] status = fs.listStatus(path);
//            for (int i=0;i<status.length;i++){
//               System.out.println(status[i].getLen());
//            }
//			
//			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(path)));			
//			
//			final String botStr = "Pingdom.com_bot";
//			AtomicInteger count = new AtomicInteger();
//			AtomicInteger validCount = new AtomicInteger();
//			br.lines().parallel().filter((String log)->{
//				count.incrementAndGet();
//				if(log.contains(botStr)){
//					return false;
//				}
//				validCount.incrementAndGet();
//				return true;
//			}).forEach((String log)-> {
//				//System.out.println(log);
//			});
//			System.out.println("total lines "+ count.get());
//			System.out.println("total valid lines "+ validCount.get());
//			
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
}
