package server.log;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.serialization.ClassResolvers;
import io.netty.handler.codec.serialization.ObjectDecoder;

public class HttpEventDataDecoder extends ObjectDecoder {
	public HttpEventDataDecoder() {
		super(ClassResolvers.weakCachingConcurrentResolver(null));		
	}
	@Override
	protected Object decode(ChannelHandlerContext ctx, ByteBuf buf)
			throws Exception {
		Object object = super.decode(ctx, buf);		
		return object;
	}
}