/**
 * Test for binary integrity through stream pipelines.
 *
 * Verifies that binary data (e.g., .tar.gz files) is NOT corrupted when:
 * 1. Sent via fileRead → httpWrite (upload)
 * 2. Received via requestBody → fileWrite (download/save)
 *
 * The bug: the dev server was buffering request bodies as strings (body += data),
 * which converts binary Buffer chunks to UTF-8, inflating and corrupting the data.
 * A 2MB gzip file would become ~3.6MB of garbage after round-tripping.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Readable } from "node:stream";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import * as http from "node:http";
import * as crypto from "node:crypto";
import { default as makeFetchHappenOriginal } from "make-fetch-happen";

// ---------------------------------------------------------------------------
// Helpers mirroring render.js
// ---------------------------------------------------------------------------

function pipeIfPossible(input, destination) {
  if (input) {
    return input.pipe(destination);
  } else {
    return destination;
  }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("binary integrity through stream pipelines", () => {
  let tmpDir;
  let server;
  let serverUrl;
  /** @type {Buffer | null} */
  let lastReceivedBody = null;

  beforeEach(async () => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-binary-stream-")
    );

    // HTTP server that captures the raw body as a Buffer
    server = http.createServer((req, res) => {
      const chunks = [];
      req.on("data", (chunk) => chunks.push(chunk));
      req.on("end", () => {
        lastReceivedBody = Buffer.concat(chunks);
        res.writeHead(200, { "Content-Type": "application/json" });
        res.end(JSON.stringify({ status: "ok", size: lastReceivedBody.length }));
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
    lastReceivedBody = null;
  });

  it("fileRead → httpWrite preserves binary data (random bytes)", async () => {
    // Create a file with random binary content (simulates .tar.gz)
    const sourceFile = path.join(tmpDir, "source.tar.gz");
    const originalData = crypto.randomBytes(1024 * 100); // 100KB random
    await fsPromises.writeFile(sourceFile, originalData);

    // Simulate: Stream.fileRead → Stream.httpWithInput (the render.js path)
    const fileStream = fs.createReadStream(sourceFile);

    const makeFetchHappen = makeFetchHappenOriginal.defaults({
      cache: "default",
    });

    const response = await makeFetchHappen(`${serverUrl}/upload`, {
      body: fileStream,
      duplex: "half",
      method: "POST",
      headers: { "Content-Type": "application/gzip" },
      timeout: 10000,
    });

    expect(response.ok).toBe(true);
    expect(lastReceivedBody).not.toBeNull();
    expect(lastReceivedBody.length).toBe(originalData.length);
    expect(lastReceivedBody.equals(originalData)).toBe(true);
  });

  it("requestBody fallback with Buffer preserves binary data", async () => {
    // Simulate the dev server's buffered body path:
    // When currentBufferedBody is a Buffer, Readable.from([buffer]) should
    // emit it as binary, not re-encode as UTF-8.
    const originalData = crypto.randomBytes(1024 * 50); // 50KB random

    // This is what render.js does for Stream.requestBody fallback:
    const stream = Readable.from([originalData]);

    // Consume the stream and verify binary integrity
    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const result = Buffer.concat(chunks);

    expect(result.length).toBe(originalData.length);
    expect(result.equals(originalData)).toBe(true);
  });

  it("requestBody fallback with string CORRUPTS binary data (demonstrates the bug)", async () => {
    // This demonstrates what was happening BEFORE the fix:
    // Binary data stored as a string gets UTF-8 re-encoded, inflating it.
    const originalData = crypto.randomBytes(1024 * 50); // 50KB random

    // Bug path: body += data converts Buffer to string (implicit UTF-8 decode)
    const corruptedString = originalData.toString(); // This is the bug!

    // Then Readable.from([string]) re-encodes as UTF-8
    const stream = Readable.from([corruptedString]);

    const chunks = [];
    for await (const chunk of stream) {
      chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const result = Buffer.concat(chunks);

    // The string round-trip inflates the data (invalid UTF-8 bytes → multi-byte sequences)
    expect(result.length).not.toBe(originalData.length);
    expect(result.equals(originalData)).toBe(false);
  });

  it("dev server body buffering preserves binary when using Buffer.concat", async () => {
    // Simulate the FIXED dev server path: buffer as Buffer[], concat at end
    const originalData = crypto.randomBytes(1024 * 100);

    // Simulate chunked arrival (like Node.js req 'data' events)
    const chunkSize = 16384;
    const bodyChunks = [];
    for (let i = 0; i < originalData.length; i += chunkSize) {
      const chunk = originalData.subarray(i, Math.min(i + chunkSize, originalData.length));
      bodyChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const bodyBuffer = Buffer.concat(bodyChunks);

    // Pass through Readable.from (the Stream.requestBody fallback)
    const stream = Readable.from([bodyBuffer]);
    const resultChunks = [];
    for await (const chunk of stream) {
      resultChunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk));
    }
    const result = Buffer.concat(resultChunks);

    expect(result.length).toBe(originalData.length);
    expect(result.equals(originalData)).toBe(true);
  });

  it("full round-trip: upload binary file and verify server receives identical bytes", async () => {
    // Create a file that looks like a real .tar.gz (starts with gzip magic bytes)
    const sourceFile = path.join(tmpDir, "real.tar.gz");
    const header = Buffer.from([0x1f, 0x8b, 0x08, 0x00]); // gzip magic
    const payload = crypto.randomBytes(1024 * 200); // 200KB random payload
    const originalData = Buffer.concat([header, payload]);
    await fsPromises.writeFile(sourceFile, originalData);

    const fileStream = fs.createReadStream(sourceFile);

    const makeFetchHappen = makeFetchHappenOriginal.defaults({
      cache: "default",
    });

    const response = await makeFetchHappen(`${serverUrl}/upload`, {
      body: fileStream,
      duplex: "half",
      method: "POST",
      headers: { "Content-Type": "application/gzip" },
      timeout: 10000,
    });

    expect(response.ok).toBe(true);

    // Verify exact byte-for-byte match
    const hash1 = crypto.createHash("sha256").update(originalData).digest("hex");
    const hash2 = crypto.createHash("sha256").update(lastReceivedBody).digest("hex");
    expect(hash2).toBe(hash1);
    expect(lastReceivedBody.length).toBe(originalData.length);
  });
});
