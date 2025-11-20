package com.gentoro.onemcp.http;

import com.gentoro.onemcp.logging.InferenceLogger;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;

public class OkHttpFactory {

  public static OkHttpClient create(String baseUrl, com.gentoro.onemcp.logging.InferenceLogger inferenceLogger) {
    if (inferenceLogger == null) {
      throw new IllegalArgumentException("InferenceLogger cannot be null");
    }
    return new OkHttpClient.Builder()
        .connectTimeout(10, TimeUnit.SECONDS)
        .readTimeout(20, TimeUnit.SECONDS)
        .addInterceptor(new BaseUrlInterceptor(baseUrl))
        .addInterceptor(new LoggingInterceptor(inferenceLogger))
        .build();
  }
}
