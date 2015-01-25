package server.log;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import rfx.core.util.netty.NettyBootstrapTemplate;
import server.http.model.HttpEventKafkaLog;

public class EventLogClient {
	String host; int port;
    public EventLogClient(String host, int port) {
		super();
		this.host = host;
		this.port = port;
	}    
    public EventLogClient send(final HttpEventKafkaLog message, final CallbackProcessor asynchCall) throws Exception{    	
    	ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new HttpEventDataDecoder());
				p.addLast(new HttpEventDataEncoder());
				p.addLast(new EventLogClientHandler(message, asynchCall));
			}
		};
		NettyBootstrapTemplate.newClientBootstrap(host, port, initializer );
    	return this;
    }  
    
    public static void main(String[] args)  {
    	HttpEventKafkaLog log = new HttpEventKafkaLog();
		log.setIp("127.0.0.1");
		log.setEventType("c");
		log.setCookieString("aa");
		log.setLogDetails("bbb");
		log.setUserAgent("ccc");
    	try {
			new EventLogClient("127.0.0.1", 14003).send(log, new CallbackProcessor() {				
				@Override
				public void process(Object obj) {
					System.out.println(obj);
				}
			});
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
   
}
