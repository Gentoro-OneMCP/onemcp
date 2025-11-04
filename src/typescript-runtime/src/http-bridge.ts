import axios, { AxiosError, AxiosInstance, AxiosRequestConfig } from "axios";
import FormData from "form-data";

/**
 * HTTP bridge used inside the isolate to perform network requests from generated SDKs.
 *
 * Behavior
 * - Always returns body as text (bodyText) so the isolate decides whether to JSON.parse.
 * - Normalizes headers to a lower-cased flat Record<string,string>.
 * - Never throws on non-2xx; instead encodes ok=false with status/bodyText.
 */

export type BridgeRequest = {
  method?: string;
  url: string; // kept for parity with fetch-like signatures; also passed as first arg
  headers?: Record<string, string>;
  // Only one of these is used:
  bodyText?: string; // serialized JSON / text
  form?: Record<string, string | Buffer>; // optional simple form
  timeout?: number; // ms
  signal?: AbortSignal; // optional cancellation
};

export type BridgeResponse = {
  ok: boolean;
  status: number;
  headers: Record<string, string>;
  bodyText: string;
};

export type HttpBridgeOptions = {
  baseURL?: string;
  timeout?: number;
  headers?: Record<string, string>;
  axiosInstance?: AxiosInstance; // optional DI for testing/mocking
};

export type RequestStubResult = {
  status: number;
  headers?: Record<string, string | string[]>;
  data?: unknown;
};

export type RequestStub = (args: {
  url: string;
  method: string;
  headers: Record<string, string>;
  data: unknown;
  timeout?: number;
  signal?: AbortSignal;
}) => Promise<RequestStubResult> | RequestStubResult;

declare global {
  // Preferred typed global stub for tests (tech-agnostic)
  // eslint-disable-next-line no-var
  var __HTTP_BRIDGE_REQUEST_STUB__: RequestStub | undefined;
  // Back-compat alias supported for older tests
  // eslint-disable-next-line no-var
  var __AXIOS_REQUEST_STUB__: RequestStub | undefined;
}

function normalizeHeaders(input?: Record<string, unknown>): Record<string, string> {
  const out: Record<string, string> = {};
  if (!input) return out;
  for (const [k, v] of Object.entries(input)) {
    if (v == null) continue;
    const key = String(k).toLowerCase();
    out[key] = Array.isArray(v) ? v.join(", ") : String(v);
  }
  return out;
}

function stringifyData(data: unknown): string {
  if (typeof data === "string") return data;
  try {
    return JSON.stringify(data ?? "");
  } catch {
    return String(data);
  }
}

export function createHttpBridge(options?: Partial<HttpBridgeOptions>): (url: string, request?: BridgeRequest) => Promise<BridgeResponse> {
  const httpClient: AxiosInstance = options?.axiosInstance ?? axios.create({
    baseURL: options?.baseURL,
    timeout: options?.timeout ?? 5000,
    headers: options?.headers,
    validateStatus: () => true,
  });

  return async (url: string, request?: BridgeRequest): Promise<BridgeResponse> => {
    const method = (request?.method ?? "GET").toUpperCase();

    // copy to avoid mutating caller headers
    const headers: Record<string, string> = { ...(request?.headers ?? {}) };

    // Ensure a UA unless provided by caller or client defaults
    const hasUserAgent = Object.keys(headers).some(h => h.toLowerCase() === "user-agent")
      || Boolean((httpClient.defaults.headers as any)?.common?.["User-Agent"] || (httpClient.defaults.headers as any)?.common?.["user-agent"]);
    if (!hasUserAgent) headers["User-Agent"] = "onemcp-http-bridge/1";

    // Build request body
    let requestBody: unknown = undefined;
    if (request?.form) {
      const formData = new FormData();
      for (const [k, v] of Object.entries(request.form)) {
        formData.append(k, v as any);
      }
      requestBody = formData;
      // Merge form headers (boundary, etc.)
      Object.assign(headers, (formData as any).getHeaders?.() ?? {});
    } else if (typeof request?.bodyText === "string") {
      requestBody = request.bodyText;
      // set a default content-type only if caller didn't set one
      const hasContentType = Object.keys(headers).some(h => h.toLowerCase() === "content-type");
      if (!hasContentType) headers["content-type"] = "text/plain;charset=utf-8";
    }

    // Prefer new stub name; fall back to legacy
    const requestStub: RequestStub | undefined = globalThis.__HTTP_BRIDGE_REQUEST_STUB__ ?? globalThis.__AXIOS_REQUEST_STUB__;
    if (typeof requestStub === "function") {
      const res = await requestStub({ url, method, headers, data: requestBody, timeout: request?.timeout, signal: request?.signal });
      return {
        ok: res.status >= 200 && res.status < 300,
        status: res.status ?? 0,
        headers: normalizeHeaders(res.headers as any),
        bodyText: stringifyData(res.data),
      };
    }

    try {
      const config: AxiosRequestConfig = {
        url,
        method,
        headers,
        data: requestBody,
        timeout: request?.timeout ?? httpClient.defaults.timeout,
        transformResponse: [(raw) => raw],
        signal: request?.signal,
      };

      const response = await httpClient.request(config);
      return {
        ok: response.status >= 200 && response.status < 300,
        status: response.status,
        headers: normalizeHeaders(response.headers as any),
        bodyText: stringifyData(response.data),
      };
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const e = error as AxiosError;
        const status = e.response?.status ?? 0;
        const headers = normalizeHeaders(e.response?.headers as any);
        const data = e.response?.data ?? { error: e.message, code: (e as any).code };
        return { ok: false, status, headers, bodyText: stringifyData(data) };
      }
      return { ok: false, status: 0, headers: {}, bodyText: stringifyData({ error: (error as Error)?.message ?? String(error) }) };
    }
  };
}

/** @deprecated Use createHttpBridge instead. */
export const makeAxiosBridge = createHttpBridge;
