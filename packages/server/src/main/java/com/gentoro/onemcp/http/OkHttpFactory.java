package com.gentoro.onemcp.http;

import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

public class OkHttpFactory {

  public static OkHttpClient create(String baseUrl) {
    return new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .addInterceptor(new BaseUrlInterceptor(baseUrl))
        .addInterceptor(new LoggingInterceptor())
        .build();
  }
}
