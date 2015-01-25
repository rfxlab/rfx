package server.log;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import rfx.core.util.netty.NettyBootstrapTemplate;
import server.http.model.HttpEventKafkaLog;

public class EventKafkaLogReceiver {
	
	public static void listen(String host, int port, final CallbackProcessor callback){
		ChannelInitializer<SocketChannel> initializer = new ChannelInitializer<SocketChannel>() {
			@Override
			public void initChannel(SocketChannel ch) throws Exception {
				ChannelPipeline p = ch.pipeline();
				p.addLast(new HttpEventDataDecoder());
				p.addLast(new HttpEventDataEncoder());								
				p.addLast(new ChannelInboundHandlerAdapter() {
					@Override
					public void channelRead(ChannelHandlerContext ctx,
							Object data) throws Exception {
						System.out.println("processed " + data);
						callback.process(data);												
						ctx.writeAndFlush(new HttpEventKafkaLog(true));
					}
				});
			}
		};
		System.out.println("EventKafkaLogReceiver " + host + ":" + port);
		NettyBootstrapTemplate.newServerBootstrap(host, port, initializer);
	}
}
