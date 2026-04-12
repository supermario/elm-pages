/**
 * Tests for streaming error handling.
 *
 * Verifies the core design decision: headers are delayed until the first
 * data chunk arrives from the stream. This means:
 *
 * - Errors BEFORE first chunk (file not found, permission denied, command
 *   not found) → proper error response with correct status code
 * - Errors AFTER first chunk (mid-stream failures) → response is truncated,
 *   same as every HTTP framework (Rails, Express, Go, Phoenix)
 *
 * Also tests the upload path where FatalError is propagated correctly
 * since the stream runs as a BackendTask before any response is sent.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Readable, Writable, PassThrough } from "node:stream";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import * as http from "node:http";

// ---------------------------------------------------------------------------
// Mock ServerResponse that tracks writeHead, write, end calls
// ---------------------------------------------------------------------------

function createMockResponse() {
  const chunks = [];
  let headInfo = null;
  let ended = false;
  let headersSentFlag = false;

  const res = new Writable({
    write(chunk, enc, cb) {
      chunks.push(chunk);
      cb();
    },
  });

  res.writeHead = (statusCode, headers) => {
    headInfo = { statusCode, headers: headers || {} };
    headersSentFlag = true;
  };
  res.setHeader = (key, value) => {
    if (!headInfo) headInfo = { statusCode: null, headers: {} };
    headInfo.headers[key] = value;
  };

  // Override end to track it
  const originalEnd = res.end.bind(res);
  res.end = (data) => {
    if (data) chunks.push(Buffer.isBuffer(data) ? data : Buffer.from(data));
    ended = true;
    originalEnd();
  };

  Object.defineProperty(res, "headersSent", {
    get: () => headersSentFlag,
  });

  return {
    res,
    getHead: () => headInfo,
    getBody: () => Buffer.concat(chunks).toString(),
    isEnded: () => ended,
  };
}

// ---------------------------------------------------------------------------
// Delayed headers behavior
// ---------------------------------------------------------------------------

describe("Streaming response — delayed headers", () => {
  it("does not write headers until first data chunk", async () => {
    const { res, getHead } = createMockResponse();
    const readable = new PassThrough();

    // Start piping but don't send data yet
    const pipePromise = new Promise((resolve) => {
      let headersSent = false;

      readable.on("data", (chunk) => {
        if (!headersSent) {
          res.writeHead(200, { "Content-Type": "application/octet-stream" });
          headersSent = true;
        }
        res.write(chunk);
      });

      readable.on("end", () => {
        if (!headersSent) {
          res.writeHead(200);
        }
        res.end();
        resolve();
      });
    });

    // Headers should NOT be sent yet
    expect(getHead()).toBeNull();

    // Now send first chunk
    readable.write("hello");
    // Small delay for event processing
    await new Promise((r) => setImmediate(r));

    // Headers should now be sent
    expect(getHead()).not.toBeNull();
    expect(getHead().statusCode).toBe(200);

    readable.end();
    await pipePromise;
  });

  it("sends error response when stream errors before first chunk", async () => {
    const { res, getHead, getBody } = createMockResponse();
    const readable = new PassThrough();

    const result = await new Promise((resolve) => {
      let headersSent = false;

      readable.on("data", (chunk) => {
        if (!headersSent) {
          res.writeHead(200);
          headersSent = true;
        }
        res.write(chunk);
      });

      readable.on("end", () => {
        res.end();
        resolve({ ok: true });
      });

      readable.on("error", (error) => {
        if (!headersSent) {
          // Can still send error response!
          resolve({ ok: false, error: error.message });
        } else {
          res.end();
          resolve({ ok: true });
        }
      });

      // Emit error before any data
      readable.destroy(new Error("ENOENT: no such file or directory"));
    });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("ENOENT");
    // Headers were NOT sent — caller can still set proper error status
    expect(getHead()).toBeNull();
  });

  it("truncates response when stream errors after first chunk", async () => {
    const { res, getHead, getBody, isEnded } = createMockResponse();
    const readable = new PassThrough();

    const resultPromise = new Promise((resolve) => {
      let headersSent = false;

      readable.on("data", (chunk) => {
        if (!headersSent) {
          res.writeHead(200, { "Content-Type": "text/plain" });
          headersSent = true;
        }
        res.write(chunk);
      });

      readable.on("end", () => {
        res.end();
        resolve({ ok: true });
      });

      readable.on("error", (error) => {
        if (!headersSent) {
          resolve({ ok: false, error: error.message });
        } else {
          // Mid-stream error — truncate
          res.end();
          resolve({ ok: true });
        }
      });
    });

    // Send some data first
    readable.write("partial data here");
    await new Promise((r) => setImmediate(r));

    // Verify headers were sent with 200
    expect(getHead().statusCode).toBe(200);

    // Now error mid-stream
    readable.destroy(new Error("Disk full"));

    const result = await resultPromise;
    // Result is "ok" because we already committed — error is in truncation
    expect(result.ok).toBe(true);
    expect(isEnded()).toBe(true);
    // Partial data was sent
    expect(getBody()).toContain("partial data");
  });
});

// ---------------------------------------------------------------------------
// Real file-based error scenarios
// ---------------------------------------------------------------------------

describe("Streaming response — file errors", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-err-")
    );
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("file not found errors before first chunk", async () => {
    const nonExistentPath = path.join(tmpDir, "does-not-exist.bin");
    const readStream = fs.createReadStream(nonExistentPath);

    const result = await new Promise((resolve) => {
      let gotData = false;

      readStream.on("data", () => {
        gotData = true;
      });

      readStream.on("error", (error) => {
        if (!gotData) {
          resolve({ ok: false, error: error.message });
        }
      });

      readStream.on("end", () => {
        resolve({ ok: true });
      });
    });

    expect(result.ok).toBe(false);
    expect(result.error).toContain("ENOENT");
  });

  it("permission denied errors before first chunk", async () => {
    const filePath = path.join(tmpDir, "no-access.bin");
    fs.writeFileSync(filePath, "secret data");
    fs.chmodSync(filePath, 0o000);

    const readStream = fs.createReadStream(filePath);

    const result = await new Promise((resolve) => {
      let gotData = false;

      readStream.on("data", () => {
        gotData = true;
      });

      readStream.on("error", (error) => {
        if (!gotData) {
          resolve({ ok: false, error: error.message });
        }
      });

      readStream.on("end", () => {
        resolve({ ok: true });
      });
    });

    // Restore permissions for cleanup
    fs.chmodSync(filePath, 0o644);

    expect(result.ok).toBe(false);
    expect(result.error).toContain("EACCES");
  });

  it("successful file stream sends all data", async () => {
    const filePath = path.join(tmpDir, "valid-file.txt");
    const content = "Hello from a valid file!";
    fs.writeFileSync(filePath, content);

    const readStream = fs.createReadStream(filePath);
    const chunks = [];

    await new Promise((resolve) => {
      readStream.on("data", (chunk) => chunks.push(chunk.toString()));
      readStream.on("end", resolve);
    });

    expect(chunks.join("")).toBe(content);
  });
});

// ---------------------------------------------------------------------------
// Upload path — FatalError before response
// ---------------------------------------------------------------------------

describe("Streaming upload — error before response", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-upload-err-")
    );
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("file write to read-only directory fails with error (not crash)", async () => {
    const readOnlyDir = path.join(tmpDir, "readonly");
    fs.mkdirSync(readOnlyDir);
    fs.chmodSync(readOnlyDir, 0o444);

    const inputData = "data to write";
    const readable = Readable.from([inputData]);
    const destPath = path.join(readOnlyDir, "output.bin");

    let caughtError = null;
    try {
      await new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(destPath);
        writeStream.on("error", reject);
        readable.pipe(writeStream);
        writeStream.on("finish", resolve);
      });
    } catch (error) {
      caughtError = error;
    }

    // Restore permissions for cleanup
    fs.chmodSync(readOnlyDir, 0o755);

    // The error was caught — in the real elm-pages flow, this becomes
    // a FatalError that the route handler can inspect and return a
    // proper error response (e.g., 500 with "upload failed" message)
    expect(caughtError).not.toBeNull();
    expect(caughtError.code).toBe("EACCES");
  });

  it("upload to valid path succeeds and allows normal response", async () => {
    const destPath = path.join(tmpDir, "uploaded.bin");
    const inputData = "uploaded content";
    const readable = Readable.from([inputData]);

    // This simulates the elm-pages flow:
    // 1. Stream pipeline runs (BackendTask)
    // 2. If success, return Response.json with 200
    // 3. If error, return Response with 500
    let streamResult;
    try {
      await new Promise((resolve, reject) => {
        const writeStream = fs.createWriteStream(destPath);
        writeStream.on("error", reject);
        readable.pipe(writeStream);
        writeStream.on("finish", resolve);
      });
      streamResult = { ok: true };
    } catch (error) {
      streamResult = { ok: false, error: error.message };
    }

    expect(streamResult.ok).toBe(true);
    expect(fs.readFileSync(destPath, "utf8")).toBe(inputData);

    // Now we can send whatever response we want — headers not committed yet
    // (In real elm-pages: BackendTask.map (\_ -> Response.json ...))
  });
});

// ---------------------------------------------------------------------------
// HTTP integration — delayed headers over real HTTP
// ---------------------------------------------------------------------------

describe("HTTP streaming — delayed headers error handling", () => {
  let tmpDir;
  let server;
  let serverPort;

  beforeEach(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "elm-pages-http-err-"));

    server = http.createServer(async (req, res) => {
      if (req.url === "/stream-existing") {
        // Stream a file that exists
        const filePath = path.join(tmpDir, "source.txt");
        const readStream = fs.createReadStream(filePath);

        let headersSent = false;
        readStream.on("data", (chunk) => {
          if (!headersSent) {
            res.writeHead(200, { "Content-Type": "text/plain" });
            headersSent = true;
          }
          res.write(chunk);
        });
        readStream.on("end", () => {
          if (!headersSent) res.writeHead(200);
          res.end();
        });
        readStream.on("error", (error) => {
          if (!headersSent) {
            res.writeHead(500, { "Content-Type": "text/plain" });
            res.end("Stream error: " + error.message);
          } else {
            res.end();
          }
        });
      } else if (req.url === "/stream-missing") {
        // Stream a file that doesn't exist
        const readStream = fs.createReadStream(
          path.join(tmpDir, "nonexistent.bin")
        );

        let headersSent = false;
        readStream.on("data", (chunk) => {
          if (!headersSent) {
            res.writeHead(200, { "Content-Type": "application/octet-stream" });
            headersSent = true;
          }
          res.write(chunk);
        });
        readStream.on("end", () => {
          if (!headersSent) res.writeHead(200);
          res.end();
        });
        readStream.on("error", (error) => {
          if (!headersSent) {
            // File not found — send proper 500 error
            res.writeHead(500, { "Content-Type": "text/plain" });
            res.end("Stream error: " + error.message);
          } else {
            res.end();
          }
        });
      } else {
        res.writeHead(404);
        res.end("Not Found");
      }
    });

    await new Promise((resolve) => {
      server.listen(0, "127.0.0.1", () => {
        serverPort = server.address().port;
        resolve();
      });
    });
  });

  afterEach(async () => {
    await new Promise((resolve) => server.close(resolve));
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("returns 200 with file content when file exists", async () => {
    const content = "This file exists and streams correctly.";
    fs.writeFileSync(path.join(tmpDir, "source.txt"), content);

    const response = await fetch(
      `http://127.0.0.1:${serverPort}/stream-existing`
    );
    expect(response.status).toBe(200);
    expect(response.headers.get("content-type")).toBe("text/plain");
    const body = await response.text();
    expect(body).toBe(content);
  });

  it("returns 500 with error when file is missing (delayed headers)", async () => {
    const response = await fetch(
      `http://127.0.0.1:${serverPort}/stream-missing`
    );

    // Because headers were delayed, we get a proper 500 — not a truncated 200!
    expect(response.status).toBe(500);
    const body = await response.text();
    expect(body).toContain("ENOENT");
  });
});
