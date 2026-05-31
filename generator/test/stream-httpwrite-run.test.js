/**
 * Test for fileRead → httpWrite pipeline with kind="none" (Stream.run).
 *
 * Reproduces the exact code path in render.js runStream() when:
 *   Stream.fileRead path
 *     |> Stream.pipe (Stream.httpWithInput { url, method, headers, ... })
 *     |> Stream.run
 *
 * The hypothesis: when httpWrite is the last part and kind="none",
 * response.body is a Readable stream. The resolution waits for "finish"
 * or "end" events on response.body, but since nobody consumes the
 * readable, "end" may never fire (stream stays paused), causing the
 * promise to hang and never resolve — which means BackendTask.andThen
 * continuations never execute.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Readable } from "node:stream";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import * as http from "node:http";
import { default as makeFetchHappenOriginal } from "make-fetch-happen";

// ---------------------------------------------------------------------------
// Helpers that mirror the render.js implementation exactly
// ---------------------------------------------------------------------------

function pipeIfPossible(input, destination) {
  if (input) {
    return input.pipe(destination);
  } else {
    return destination;
  }
}

function jsonResponse(req, value) {
  return value;
}

/**
 * Simulate the render.js httpWrite handler (lines 1834-1863).
 */
async function httpWritePart(lastStream, part, resolve, isLastProcess, kind) {
  const makeFetchHappen = makeFetchHappenOriginal.defaults({
    cache: "default",
  });
  const response = await makeFetchHappen(part.url, {
    body: lastStream,
    duplex: "half",
    redirect: "follow",
    method: part.method,
    headers: part.headers || {},
    retry: part.retries,
    timeout: part.timeoutInMs,
  });
  if (!isLastProcess && !response.ok) {
    resolve({
      error: `HTTP request failed: ${response.status} ${response.statusText}`,
    });
  } else {
    let metadata = () => {
      return {
        headers: Object.fromEntries(response.headers.entries()),
        statusCode: response.status,
        url: response.url,
        statusText: response.statusText,
      };
    };
    return { metadata, stream: response.body };
  }
}

/**
 * Simulate the render.js runStream() resolution for kind="none" (lines 1632-1654).
 * This is the exact code path that Stream.run uses.
 */
function resolveNoneKind(lastStream, metadataResponse, resolve) {
  if (!lastStream) {
    resolve({ body: null });
  } else {
    // tryCallingFunction equivalent
    let resolvedMeta =
      typeof metadataResponse === "function"
        ? metadataResponse()
        : metadataResponse;

    let resolved = false;
    const onComplete = () => {
      if (resolved) return;
      resolved = true;
      resolve({
        body: null,
        metadata: resolvedMeta,
      });
    };
    lastStream.once("finish", onComplete);
    lastStream.once("end", onComplete);
    // Fix: drain readable streams so "end" fires
    if (typeof lastStream.resume === "function" && lastStream.readable) {
      lastStream.resume();
    }
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("fileRead → httpWrite with Stream.run (kind=none)", () => {
  let tmpDir;
  let server;
  let serverUrl;

  beforeEach(async () => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-httpwrite-")
    );

    // Create a small test file
    const testFile = path.join(tmpDir, "test-slug.tar.gz");
    await fsPromises.writeFile(testFile, "hello world this is a test slug");

    // Start a local HTTP server that accepts POST and returns JSON
    server = http.createServer((req, res) => {
      let body = [];
      req.on("data", (chunk) => body.push(chunk));
      req.on("end", () => {
        const received = Buffer.concat(body).toString();
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "ok", received: received.length }));
      });
    });

    await new Promise((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        const addr = server.address();
        serverUrl = `http://127.0.0.1:${addr.port}`;
        resolve();
      });
    });
  });

  afterEach(async () => {
    if (server) {
      await new Promise((resolve) => server.close(resolve));
    }
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("should resolve the promise after httpWrite completes (reproduces hang)", async () => {
    const testFile = path.join(tmpDir, "test-slug.tar.gz");

    // Step 1: fileRead — same as render.js line 1720
    const fileStream = fs.createReadStream(testFile);

    // Step 2: httpWrite — same as render.js lines 1834-1863
    const result = await new Promise(async (outerResolve, outerReject) => {
      // Timeout to detect hang — if this fires, the promise never resolved
      const timeout = setTimeout(() => {
        outerReject(
          new Error(
            "HANG DETECTED: Stream.run promise never resolved after httpWrite. " +
              "This confirms the bug — response.body readable stream never emits 'end' " +
              "when nobody consumes it (kind='none')."
          )
        );
      }, 5000);

      const resolve = (value) => {
        clearTimeout(timeout);
        outerResolve(value);
      };

      try {
        const { stream: responseBodyStream, metadata } = await httpWritePart(
          fileStream,
          {
            url: `${serverUrl}/upload`,
            method: "POST",
            headers: [{ key: "Content-Type", value: "application/gzip" }],
          },
          resolve,
          true, // isLastProcess
          "none"
        );

        // Step 3: kind="none" resolution — same as render.js lines 1632-1654
        resolveNoneKind(responseBodyStream, metadata, resolve);
      } catch (error) {
        clearTimeout(timeout);
        outerReject(error);
      }
    });

    // If we get here, the promise resolved — no hang
    expect(result).toBeTruthy();
    expect(result.body).toBeNull();
  });

  it("should resolve even with a larger file", async () => {
    // Write a larger file (1MB) to rule out small-body edge cases
    const largeFile = path.join(tmpDir, "large-slug.tar.gz");
    const largeData = Buffer.alloc(1024 * 1024, 0x42);
    await fsPromises.writeFile(largeFile, largeData);

    const fileStream = fs.createReadStream(largeFile);

    const result = await new Promise(async (outerResolve, outerReject) => {
      const timeout = setTimeout(() => {
        outerReject(
          new Error(
            "HANG DETECTED: Stream.run promise never resolved after httpWrite with large file."
          )
        );
      }, 10000);

      const resolve = (value) => {
        clearTimeout(timeout);
        outerResolve(value);
      };

      try {
        const { stream: responseBodyStream, metadata } = await httpWritePart(
          fileStream,
          {
            url: `${serverUrl}/upload`,
            method: "POST",
            headers: [{ key: "Content-Type", value: "application/gzip" }],
          },
          resolve,
          true,
          "none"
        );

        resolveNoneKind(responseBodyStream, metadata, resolve);
      } catch (error) {
        clearTimeout(timeout);
        outerReject(error);
      }
    });

    expect(result).toBeTruthy();
    expect(result.body).toBeNull();
  });
});
