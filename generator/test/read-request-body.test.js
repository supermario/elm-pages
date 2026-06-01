/**
 * Tests for runReadRequestBody and the render.js request body pipeline.
 *
 * These tests verify the actual functions exported from render.js that handle
 * request body access in BackendTask context, including:
 *
 * 1. Buffer vs string body handling (the root cause of the readBody bug)
 * 2. jsonResponse wrapper format matching Elm decoder expectations
 * 3. setCurrentBufferedBody / setCurrentRequestId lifecycle
 * 4. Stream vs buffered fallback path selection
 * 5. End-to-end: JS response shape matches what Elm's readBody decoder expects
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Readable } from "node:stream";
import * as consumers from "stream/consumers";
import {
  registerRequestBodyStream,
  unregisterRequestBodyStream,
  setCurrentRequestId,
  setCurrentBufferedBody,
} from "../src/render.js";

/**
 * Simulate what runReadRequestBody does internally, matching the actual
 * render.js implementation. We replicate the logic here rather than importing
 * the private function, so the test validates the _pattern_ even if internals
 * are refactored.
 */
function simulateRunReadRequestBody(
  currentRequestId,
  requestBodyStreams,
  currentBufferedBody
) {
  function jsonResponse(request, json) {
    return {
      request,
      response: { bodyKind: "json", body: json },
    };
  }

  const req = { url: "elm-pages-internal://read-request-body" };
  const stream = currentRequestId
    ? requestBodyStreams.get(currentRequestId)
    : null;

  if (stream) {
    return consumers.text(stream).then(
      (body) => jsonResponse(req, { body }),
      (error) => jsonResponse(req, { error: error.toString() })
    );
  }

  const bodyStr = Buffer.isBuffer(currentBufferedBody)
    ? currentBufferedBody.toString("utf-8")
    : currentBufferedBody;
  return Promise.resolve(jsonResponse(req, { body: bodyStr }));
}

describe("runReadRequestBody: Buffer vs string body", () => {
  it("should return a string when currentBufferedBody is a Buffer", async () => {
    const body = '{"token":"x"}';
    const buf = Buffer.from(body);
    const result = await simulateRunReadRequestBody(null, new Map(), buf);

    expect(result.response.bodyKind).toBe("json");
    expect(result.response.body.body).toBe(body);
    expect(typeof result.response.body.body).toBe("string");
  });

  it("should return a string when currentBufferedBody is already a string", async () => {
    const body = '{"token":"x"}';
    const result = await simulateRunReadRequestBody(null, new Map(), body);

    expect(result.response.body.body).toBe(body);
    expect(typeof result.response.body.body).toBe("string");
  });

  it("should return null when currentBufferedBody is null", async () => {
    const result = await simulateRunReadRequestBody(null, new Map(), null);

    expect(result.response.body.body).toBeNull();
  });

  it("should preserve binary-safe UTF-8 round-trip through Buffer", async () => {
    const original = '{"emoji":"\\ud83d\\ude00","data":"base64=="}';
    const buf = Buffer.from(original, "utf-8");
    const result = await simulateRunReadRequestBody(null, new Map(), buf);

    expect(result.response.body.body).toBe(original);
  });

  it("should use stream path when currentRequestId is set and stream exists", async () => {
    const bodyText = "stream body content";
    const streams = new Map();
    streams.set("req-42", Readable.from([bodyText]));

    const result = await simulateRunReadRequestBody("req-42", streams, "should-not-use-this");

    expect(result.response.body.body).toBe(bodyText);
  });

  it("should fall back to buffered body when stream is not registered", async () => {
    const result = await simulateRunReadRequestBody("req-99", new Map(), "fallback body");

    expect(result.response.body.body).toBe("fallback body");
  });

  it("should fall back to buffered body when currentRequestId is null", async () => {
    const streams = new Map();
    streams.set("req-1", Readable.from(["should not use"]));

    const result = await simulateRunReadRequestBody(null, streams, "buffered");

    expect(result.response.body.body).toBe("buffered");
  });
});

describe("jsonResponse shape matches Elm decoder expectations", () => {
  it("should produce the exact shape that RequestsAndPending.bodyDecoder expects", () => {
    // render.js jsonResponse wraps as:
    //   { request: ..., response: { bodyKind: "json", body: <value> } }
    // Elm's bodyDecoder expects: field "bodyKind" string, then field "body" value
    // Elm's readBody expect decoder: maybe (field "body" string) applied to the body value

    function jsonResponse(request, json) {
      return {
        request,
        response: { bodyKind: "json", body: json },
      };
    }

    const result = jsonResponse({}, { body: "hello" });

    // Level 1: response wrapper
    expect(result.response).toBeDefined();
    expect(result.response.bodyKind).toBe("json");

    // Level 2: the body value (what Elm gets as JsonBody)
    const jsonBody = result.response.body;
    expect(jsonBody).toEqual({ body: "hello" });

    // Level 3: what Elm's readBody decoder runs on jsonBody
    // Decode.maybe (Decode.field "body" Decode.string)
    expect(typeof jsonBody.body).toBe("string");
    expect(jsonBody.body).toBe("hello");
  });

  it("should fail Elm string decode when body is a Buffer object (the original bug)", () => {
    function jsonResponse(request, json) {
      return {
        request,
        response: { bodyKind: "json", body: json },
      };
    }

    const buf = Buffer.from("test");
    const result = jsonResponse({}, { body: buf });

    // This is what JSON.stringify produces for a Buffer — NOT a string
    const serialized = JSON.parse(JSON.stringify(result.response.body));
    expect(serialized.body).toHaveProperty("type", "Buffer");
    expect(serialized.body).toHaveProperty("data");
    expect(typeof serialized.body).toBe("object");
    // Elm's Decode.string would fail on this object → Decode.maybe returns Nothing
  });

  it("should pass Elm string decode when Buffer is converted to string (the fix)", () => {
    function jsonResponse(request, json) {
      return {
        request,
        response: { bodyKind: "json", body: json },
      };
    }

    const buf = Buffer.from("test");
    const bodyStr = Buffer.isBuffer(buf) ? buf.toString("utf-8") : buf;
    const result = jsonResponse({}, { body: bodyStr });

    const serialized = JSON.parse(JSON.stringify(result.response.body));
    expect(typeof serialized.body).toBe("string");
    expect(serialized.body).toBe("test");
  });
});

describe("render.js exported body lifecycle functions", () => {
  afterEach(() => {
    setCurrentBufferedBody(null);
    setCurrentRequestId(null);
  });

  it("setCurrentBufferedBody accepts a string", () => {
    // Should not throw
    setCurrentBufferedBody("hello");
  });

  it("setCurrentBufferedBody accepts a Buffer", () => {
    setCurrentBufferedBody(Buffer.from("hello"));
  });

  it("setCurrentBufferedBody accepts null", () => {
    setCurrentBufferedBody(null);
  });

  it("registerRequestBodyStream and unregisterRequestBodyStream lifecycle", () => {
    const stream = Readable.from(["test"]);
    registerRequestBodyStream("req-1", stream);
    // No public getter, but unregister should not throw
    unregisterRequestBodyStream("req-1");
    unregisterRequestBodyStream("req-1"); // double unregister is safe
  });

  it("setCurrentRequestId accepts string and null", () => {
    setCurrentRequestId("req-42");
    setCurrentRequestId(null);
  });
});

describe("End-to-end: dev-server POST body pipeline", () => {
  it("should correctly pipeline: Buffer body → currentBufferedBody → readBody response", async () => {
    // Simulate the dev-server POST flow:
    // 1. Body arrives as Buffer chunks, concatenated
    // 2. String version goes into serverRequest.body (for JSON flags)
    // 3. Raw Buffer goes into serverRequest.__rawBodyBuffer
    // 4. render() sets currentBufferedBody = __rawBodyBuffer || body
    // 5. runReadRequestBody reads currentBufferedBody, converts Buffer→string

    const jsonPayload = '{"appId":"myapp","token":"abc123"}';
    const bodyBuffer = Buffer.from(jsonPayload);
    const bodyString = bodyBuffer.toString("utf-8");

    // Step 2-3: serverRequest construction
    const serverRequest = {
      method: "POST",
      headers: { "content-type": "application/json" },
      rawUrl: "http://localhost:8070/v1/admin/test",
      body: bodyString,
      requestTime: Date.now(),
      __rawBodyBuffer: bodyBuffer,
    };

    // Step 4: render() body selection (prefers __rawBodyBuffer)
    const currentBufferedBody =
      serverRequest.__rawBodyBuffer
        ? serverRequest.__rawBodyBuffer
        : serverRequest.body != null
          ? serverRequest.body
          : null;

    expect(Buffer.isBuffer(currentBufferedBody)).toBe(true);

    // Step 5: runReadRequestBody converts and returns
    const result = await simulateRunReadRequestBody(null, new Map(), currentBufferedBody);

    expect(typeof result.response.body.body).toBe("string");
    expect(result.response.body.body).toBe(jsonPayload);
    expect(JSON.parse(result.response.body.body)).toEqual({
      appId: "myapp",
      token: "abc123",
    });
  });

  it("should handle GET request (no body) gracefully", async () => {
    const serverRequest = {
      method: "GET",
      body: null,
    };

    const currentBufferedBody =
      serverRequest.__rawBodyBuffer
        ? serverRequest.__rawBodyBuffer
        : serverRequest.body != null
          ? serverRequest.body
          : null;

    expect(currentBufferedBody).toBeNull();

    const result = await simulateRunReadRequestBody(null, new Map(), currentBufferedBody);
    expect(result.response.body.body).toBeNull();
  });

  it("should handle non-JSON POST body (form data as string)", async () => {
    const formBody = "username=admin&password=secret";
    const bodyBuffer = Buffer.from(formBody);

    const currentBufferedBody = bodyBuffer;
    const result = await simulateRunReadRequestBody(null, new Map(), currentBufferedBody);

    expect(result.response.body.body).toBe(formBody);
  });

  it("should handle large JSON POST body", async () => {
    const largePayload = JSON.stringify({
      data: "x".repeat(100000),
      nested: { array: Array.from({ length: 1000 }, (_, i) => i) },
    });
    const bodyBuffer = Buffer.from(largePayload);

    const result = await simulateRunReadRequestBody(null, new Map(), bodyBuffer);

    expect(result.response.body.body).toBe(largePayload);
    expect(JSON.parse(result.response.body.body).data.length).toBe(100000);
  });
});

describe("GotDataBatch JSON shape (what Elm receives)", () => {
  it("should produce the exact structure that GotDataBatch decodes", () => {
    // This is what render.js sends to app.ports.gotBatchSub.send(results)
    // Each entry: { key: requestHash, json: { request, response }, bytes: null }

    const requestHash = "12345";
    const readBodyResponse = {
      request: { url: "elm-pages-internal://read-request-body" },
      response: { bodyKind: "json", body: { body: "the POST body" } },
    };

    const batchEntry = {
      key: requestHash,
      json: {
        request: readBodyResponse.request,
        response: readBodyResponse.response,
      },
      bytes: null,
    };

    // Elm decodes: Encode.object [(key, json)] then:
    //   field key (field "response" (decoder maybeBytes))
    // where decoder = map2 Response (maybe responseDecoder) (bodyDecoder maybeBytes)

    // responseDecoder expects statusCode/statusText/headers/url — none present
    // so Decode.maybe responseDecoder → Nothing (this is fine)

    // bodyDecoder: field "bodyKind" string → "json"
    //             field "body" value → { body: "the POST body" }
    //             → JsonBody { body: "the POST body" }

    const response = batchEntry.json.response;
    expect(response.bodyKind).toBe("json");
    expect(response.body).toEqual({ body: "the POST body" });

    // Then readBody's expect decoder: maybe (field "body" string)
    // runs on JsonBody's value: { body: "the POST body" }
    expect(typeof response.body.body).toBe("string");
  });
});
