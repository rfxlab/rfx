package rfx.query.netty;


import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.protobuf.ProtobufDecoder;
import io.netty.handler.codec.protobuf.ProtobufEncoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32FrameDecoder;
import io.netty.handler.codec.protobuf.ProtobufVarint32LengthFieldPrepender;
import io.netty.handler.ssl.SslContext;

public class ActorMasterChannelInitializer extends ChannelInitializer<SocketChannel> {

    private final SslContext sslCtx;

    public ActorMasterChannelInitializer(SslContext sslCtx) {
        this.sslCtx = sslCtx;
       // Persons.Person person = Persons.Person.newBuilder().setActorId(100L).setName("aa").build();
    }

    @Override
    public void initChannel(SocketChannel ch) {
        ChannelPipeline p = ch.pipeline();
        if (sslCtx != null) {
            p.addLast(sslCtx.newHandler(ch.alloc(), ActorDataNode.HOST, ActorDataNode.PORT));
        }

        p.addLast(new ProtobufVarint32FrameDecoder());
        p.addLast(new ProtobufDecoder(QueryProtocol.LocalTimes.getDefaultInstance()));

        p.addLast(new ProtobufVarint32LengthFieldPrepender());
        p.addLast(new ProtobufEncoder());

        p.addLast(new ActorMasterChannelHandler());
    }
}