package com.gentoro.onemcp.http;

import java.io.IOException;
import okhttp3.*;
import okio.Buffer;
import org.jetbrains.annotations.NotNull;

public class LoggingInterceptor implements Interceptor {
  private static final org.slf4j.Logger log =
      com.gentoro.onemcp.logging.LoggingService.getLogger(LoggingInterceptor.class);

  @NotNull
  @Override
  public Response intercept(Chain chain) throws IOException {
    Request request = chain.request();

    long startTime = System.nanoTime();
    log.debug(
        "➡️ Sending request {}\nHeaders:\n{}\nBody:\n{}\n",
        request.url(),
        request.headers(),
        bodyToString(request));

    Response response = chain.proceed(request);

    long endTime = System.nanoTime();
    log.debug(
        "⬅️ Received response for {} in {} ms\nStatus: {}\nHeaders:\n{}\n",
        response.request().url(),
        String.format("%.1f", (endTime - startTime) / 1e6d),
        response.code(),
        response.headers());

    // Read body safely (clone it)
    ResponseBody responseBody = response.peekBody(Long.MAX_VALUE);
    log.trace("Response body:\n{}\n", responseBody.string());

    return response;
  }

  private static String bodyToString(Request request) {
    try {
      Request copy = request.newBuilder().build();
      Buffer buffer = new Buffer();
      if (copy.body() != null) copy.body().writeTo(buffer);
      return buffer.readUtf8();
    } catch (IOException e) {
      return "(error reading body)";
    }
  }
}
