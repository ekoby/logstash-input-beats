package org.openziti.logstash.beats;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.timeout.IdleStateHandler;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.EventExecutorGroup;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.logstash.beats.*;
import org.openziti.Ziti;
import org.openziti.ZitiAddress;
import org.openziti.ZitiContext;
import org.openziti.api.Service;
import org.openziti.netty.ZitiServerChannel;
import org.openziti.netty.ZitiServerChannelFactory;

public class Server extends org.logstash.beats.Server {

    private final static Logger logger = LogManager.getLogger(Server.class);

    private EventLoopGroup workGroup;
    private final String serviceName;

    private final ZitiContext ztx;

    public Server(String zitiId, String service, int clientInactivityTimeoutSeconds, int threadCount) {
        super(service, 0, clientInactivityTimeoutSeconds, threadCount);
        this.serviceName = service;
        this.ztx = Ziti.newContext(zitiId, new char[0]);
    }

    @Override
    public org.logstash.beats.Server listen() throws InterruptedException {
        if (workGroup != null) {
            try {
                logger.debug("Shutting down existing worker group before starting");
                workGroup.shutdownGracefully().sync();
            } catch (Exception e) {
                logger.error("Could not shut down worker group before starting", e);
            }
        }
        workGroup = new DefaultEventLoopGroup();
        try {
            logger.info("Starting server for service: {}", this.serviceName);

            ServerBootstrap server = new ServerBootstrap();
            server.group(workGroup)
                    .channelFactory(new ZitiServerChannelFactory(ztx))
                    .childHandler(getBeatsInitializer());

            Service service = ztx.getService(serviceName, 10000L);
            Channel channel = server.bind(new ZitiAddress.Bind(service.getName())).sync().channel();
            channel.closeFuture().sync();
        } finally {
            shutdown();
        }

        return this;
    }

//    private class BeatsInitializer extends ChannelInitializer<Channel> {
//        private final String SSL_HANDLER = "ssl-handler";
//        private final String IDLESTATE_HANDLER = "idlestate-handler";
//        private final String CONNECTION_HANDLER = "connection-handler";
//        private final String BEATS_ACKER = "beats-acker";
//
//
//        private final int DEFAULT_IDLESTATEHANDLER_THREAD = 4;
//        private final int IDLESTATE_WRITER_IDLE_TIME_SECONDS = 5;
//
//        private final EventExecutorGroup idleExecutorGroup;
//        private final EventExecutorGroup beatsHandlerExecutorGroup;
//        private final IMessageListener localMessageListener;
//        private final int localClientInactivityTimeoutSeconds;
//
//        BeatsInitializer(IMessageListener messageListener, int clientInactivityTimeoutSeconds, int beatsHandlerThread) {
//            // Keeps a local copy of Server settings, so they can't be modified once it starts listening
//            this.localMessageListener = messageListener;
//            this.localClientInactivityTimeoutSeconds = clientInactivityTimeoutSeconds;
//            idleExecutorGroup = new DefaultEventExecutorGroup(DEFAULT_IDLESTATEHANDLER_THREAD);
//            beatsHandlerExecutorGroup = new DefaultEventExecutorGroup(beatsHandlerThread);
//        }
//
//        public void initChannel(Channel socket){
//            ChannelPipeline pipeline = socket.pipeline();
//
//            if (isSslEnabled()) {
//                pipeline.addLast(SSL_HANDLER, sslHandlerProvider.sslHandlerForChannel(socket));
//            }
//            pipeline.addLast(idleExecutorGroup, IDLESTATE_HANDLER,
//                    new IdleStateHandler(localClientInactivityTimeoutSeconds, IDLESTATE_WRITER_IDLE_TIME_SECONDS, localClientInactivityTimeoutSeconds));
//            pipeline.addLast(BEATS_ACKER, new AckEncoder());
//            pipeline.addLast(CONNECTION_HANDLER, new ConnectionHandler());
//            pipeline.addLast(beatsHandlerExecutorGroup, new BeatsParser(), new BeatsHandler(localMessageListener));
//        }
//
//
//
//        @Override
//        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
//            logger.warn("Exception caught in channel initializer", cause);
//            try {
//                localMessageListener.onChannelInitializeException(ctx, cause);
//            } finally {
//                super.exceptionCaught(ctx, cause);
//            }
//        }
//
//        public void shutdownEventExecutor() {
//            try {
//                idleExecutorGroup.shutdownGracefully().sync();
//                beatsHandlerExecutorGroup.shutdownGracefully().sync();
//            } catch (InterruptedException e) {
//                throw new IllegalStateException(e);
//            }
//        }
//    }
public static void main(String[] args) {
    var ztx = Ziti.newContext(args[0], new char[0]);
    var server = new Server(args[0], args[1], 15, Runtime.getRuntime().availableProcessors());
    try {
        server.listen();
    } catch (Exception ex) {
        ex.printStackTrace();
    }
}
}
