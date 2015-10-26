import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Handles a server-side channel.
 */
public final class FrontendServerHandler extends ChannelInboundHandlerAdapter { // (1)

    private HttpRequest request;
    /** Buffer that stores the response content */

    static final String team_info = "Omegaga's Black Railgun,6537-0651-1730\n";

    @Override
    public void channelReadComplete(ChannelHandlerContext ctx) {
        ctx.flush();
    }

    private String buildResponse( String userId, String timestamp ){
        String answer = SQLHandler.getSqlAnswers(userId, timestamp);
        if( answer == null )
            return null;
        return team_info + SQLHandler.getSqlAnswers(userId, timestamp);
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
        if (msg instanceof HttpRequest) {
            HttpRequest request = this.request = (HttpRequest) msg;
            QueryStringDecoder queryStringDecoder =
                    new QueryStringDecoder(request.getUri());
            final Map<String, List<String>> params = queryStringDecoder.parameters();
            if (!params.isEmpty()) {
                String buf = null;
                try {
                    buf = buildResponse(params.get("userid").get(0),
                                    params.get("tweet_time").get(0));
                } catch (Exception e) {
                    // Ugly catch... In case the HTTP request is in bad format
                    e.printStackTrace();
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE);
                }
                if (!writeResponse(buf, ctx)) {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE);
                }
            }
        }
    }


    private boolean writeResponse(final String buf, final ChannelHandlerContext ctx) {
        // Decide whether to close the connection or not.
        final boolean keepAlive = HttpHeaders.isKeepAlive(request);
        // Build the response object.
        final FullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK,
                Unpooled.copiedBuffer(buf, CharsetUtil.UTF_8));

        response.headers().set(Names.CONTENT_TYPE, "text/plain; charset=UTF-8");

        if (keepAlive) {
            // Add 'Content-Length' header only for a keep-alive connection.
            response.headers().set(Names.CONTENT_LENGTH,response.content().readableBytes());
            // Add keep alive header as per:
            // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            response.headers().set(Names.CONNECTION, HttpHeaders.Values.KEEP_ALIVE);
        }

        // Write the response.
        ctx.write(response);

        return keepAlive;
    }


    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) { // (4)
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }
}
