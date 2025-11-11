package com.gentoro.onemcp.http;

import java.io.IOException;
import okhttp3.*;
import org.jetbrains.annotations.NotNull;

public class BaseUrlInterceptor implements Interceptor {
  private final String baseUrl;

  public BaseUrlInterceptor(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  @NotNull
  @Override
  public Response intercept(Chain chain) throws IOException {
    Request original = chain.request();
    HttpUrl originalUrl = original.url();

    // Example: only rewrite if the host is missing or matches placeholder
    if (originalUrl.host().isEmpty()) {
      HttpUrl newUrl = HttpUrl.get(baseUrl).resolve(originalUrl.encodedPath());

      Request newRequest = original.newBuilder().url(newUrl).build();

      return chain.proceed(newRequest);
    }

    return chain.proceed(original);
  }
}
