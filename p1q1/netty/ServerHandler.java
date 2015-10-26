/**
 * Created by omega on 10/18/15.
 */
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import io.netty.handler.codec.http.HttpHeaders.Names;
import io.netty.handler.codec.http.HttpResponseStatus;

import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.List;
import java.util.Map;

/**
 * Handles a server-side channel.
 */
public final class ServerHandler extends ChannelInboundHandlerAdapter { // (1)

  private HttpRequest request;
  /** Buffer that stores the response content */

  static final String team_info = "Omegaga's Black Railgun,6537-0651-1730\n";
  static final BigInteger X = new BigInteger("8271997208960872478735181815578166723519929177896558845922250595511921395049126920528021164569045773");

  static String decode(String msg, int shift){
    final int n = msg.length();
    final int k = (int)Math.sqrt(n);
    String ret = "";

    int j;
    for (int sum = 0; sum < k; sum++) {
      j = sum;
      for(int x = 0; x <= sum; x++) {
        // add character from msg to ret, transform according to Caesar decrypt
        ret += (char)('A' + ((msg.charAt(j) - 'A' + 26 - shift) % 26));
        j += k - 1;
      }
    }

    int init = (k << 1) - 1;
    for (int sum = k - 2; sum >= 0; sum--){
      j = init;
      for (int x = 0; x <= sum; x++) {
        // add character from msg to ret, transform according to Caesar decrypt
        ret += (char)('A' + ((msg.charAt(j) - 'A' + 26 - shift) % 26));
        j += k - 1;
      }
      init += k;
    }

    return ret;
  }

  static String retStringGenerator(String key, String msg) {
    final BigInteger keyInt = new BigInteger(key);
    final BigInteger y = keyInt.divide(X);

    final int z = y.mod(new BigInteger("25")).intValue() + 1;
    final Calendar cal = Calendar.getInstance();
    final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss\n");
    return team_info + sdf.format(cal.getTime()) + decode(msg, z) + "\n";
  }

  @Override
  public void channelReadComplete(ChannelHandlerContext ctx) {
    ctx.flush();
  }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) { // (2)
      if (msg instanceof HttpRequest) {
        HttpRequest request = this.request = (HttpRequest) msg;
        QueryStringDecoder queryStringDecoder =
            new QueryStringDecoder(request.getUri());
        final Map<String, List<String>> params = queryStringDecoder.parameters();
        if (!params.isEmpty()) {
          final String buf =
              retStringGenerator(params.get("key").get(0),
                  params.get("message").get(0));
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
