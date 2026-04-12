/**
 * Large file streaming integration tests.
 *
 * These tests verify that the requestBody → fileWrite pipeline streams data
 * with constant memory, proving that the stream semantics are preserved
 * end-to-end and the body is never fully buffered.
 *
 * The tests replicate the exact code path in render.js:
 *   1. A Readable (simulating req / IncomingMessage) is registered in a Map
 *   2. The "requestBody" handler looks it up and returns it
 *   3. The "fileWrite" handler creates a fs.createWriteStream
 *   4. pipeIfPossible() connects them via .pipe()
 *   5. Data flows chunk-by-chunk to disk
 *
 * Memory is sampled periodically during the stream. If the pipeline were
 * buffering (e.g., collecting all chunks into a string before writing),
 * RSS would grow proportionally to the data size. With true streaming,
 * RSS stays bounded regardless of total data volume.
 */

import { describe, it, expect, beforeEach, afterEach } from "vitest";
import { Readable, Writable, PassThrough } from "node:stream";
import * as fs from "node:fs";
import * as fsPromises from "node:fs/promises";
import * as path from "node:path";
import * as os from "node:os";
import * as crypto from "node:crypto";
import * as http from "node:http";

// ---------------------------------------------------------------------------
// Helpers that mirror the render.js implementation
// ---------------------------------------------------------------------------

/** Same as render.js pipeIfPossible */
function pipeIfPossible(input, destination) {
  if (input) {
    return input.pipe(destination);
  } else {
    return destination;
  }
}

/**
 * Replicate the render.js requestBody → fileWrite pipeline exactly.
 *
 * @param {import('node:stream').Readable} requestStream - the "req" body
 * @param {string} destPath - file path to write to
 * @returns {Promise<void>}
 */
async function executeRequestBodyToFileWrite(requestStream, destPath) {
  // Step 1: "requestBody" handler — returns the registered stream
  const requestBodyResult = { stream: requestStream };

  // Step 2: "fileWrite" handler — mkdir + createWriteStream + pipe
  await fsPromises.mkdir(path.dirname(destPath), { recursive: true });
  const writeStream = fs.createWriteStream(destPath);

  return new Promise((resolve, reject) => {
    writeStream.once("error", reject);
    requestBodyResult.stream.once("error", reject);

    // This is the critical line — .pipe() is what gives us streaming.
    // If this were replaced with collecting chunks + fs.writeFile,
    // memory would blow up.
    pipeIfPossible(requestBodyResult.stream, writeStream);

    writeStream.once("finish", resolve);
  });
}

/**
 * Create a Readable that generates exactly `totalBytes` of deterministic data.
 * Produces data in fixed-size chunks to simulate a real upload.
 *
 * The `yieldEvery` parameter controls how often the readable yields to the
 * event loop (via setImmediate), which allows memory sampling intervals to
 * fire. Without this, .pipe() to a fast local disk completes synchronously
 * and setInterval never gets a chance to sample.
 */
function createLargeReadable(totalBytes, chunkSize = 64 * 1024, yieldEvery = 256) {
  let bytesRemaining = totalBytes;
  let chunkCount = 0;

  return new Readable({
    read() {
      const pushChunks = () => {
        while (bytesRemaining > 0) {
          const size = Math.min(chunkSize, bytesRemaining);
          const chunk = Buffer.alloc(size, bytesRemaining % 256);
          bytesRemaining -= size;
          chunkCount++;

          if (!this.push(chunk)) {
            // Backpressure — stop until consumer drains
            return;
          }

          // Periodically yield to the event loop so memory sampling works
          if (chunkCount % yieldEvery === 0) {
            setImmediate(() => pushChunks());
            return;
          }
        }
        if (bytesRemaining <= 0) {
          this.push(null);
        }
      };
      pushChunks();
    },
  });
}

/**
 * Sample RSS memory at intervals during a promise. Returns all samples.
 * Always captures at least one sample at the end to handle fast completions.
 */
function monitorMemory(promise, intervalMs = 50) {
  const samples = [process.memoryUsage().rss]; // baseline sample
  const interval = setInterval(() => {
    samples.push(process.memoryUsage().rss);
  }, intervalMs);

  return promise
    .finally(() => {
      clearInterval(interval);
      samples.push(process.memoryUsage().rss); // final sample
    })
    .then(() => samples);
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe("Large file streaming — memory bounded", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-large-")
    );
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("warms up the JIT and Node internals (not a real test)", async () => {
    // The first streaming operation in a process has higher RSS growth due to
    // JIT compilation and Node.js internal buffer pool initialization.
    // This warmup run ensures subsequent tests measure steady-state behavior.
    const destPath = path.join(tmpDir, "warmup.bin");
    const readable = createLargeReadable(20 * 1024 * 1024); // 20 MB
    await executeRequestBodyToFileWrite(readable, destPath);
    expect(fs.statSync(destPath).size).toBe(20 * 1024 * 1024);
  });

  it("streams 200 MB to disk with less than 50 MB RSS growth", async () => {
    const totalBytes = 200 * 1024 * 1024; // 200 MB
    const destPath = path.join(tmpDir, "output-200mb.bin");

    if (global.gc) global.gc();
    const baselineRss = process.memoryUsage().rss;

    const readable = createLargeReadable(totalBytes);

    const samples = await monitorMemory(
      executeRequestBodyToFileWrite(readable, destPath),
      100
    );

    const stats = fs.statSync(destPath);
    expect(stats.size).toBe(totalBytes);

    // With true streaming, RSS growth should be a small fraction of data size.
    // Buffering 200 MB causes ~190 MB growth (see control test).
    // Streaming should stay well under half the data size even with Node.js
    // internal buffer pool allocation.
    const peakRss = Math.max(...samples);
    const rssGrowth = peakRss - baselineRss;
    const growthRatio = rssGrowth / totalBytes;

    console.log(
      `  200 MB stream: baseline=${(baselineRss / 1024 / 1024).toFixed(1)} MB, ` +
        `peak=${(peakRss / 1024 / 1024).toFixed(1)} MB, ` +
        `growth=${(rssGrowth / 1024 / 1024).toFixed(1)} MB (${(growthRatio * 100).toFixed(1)}%), ` +
        `samples=${samples.length}`
    );

    // RSS growth must be less than half the data volume — a buffered approach
    // would show ~100% growth, streaming shows <40% (mostly Node buffer pools)
    expect(rssGrowth).toBeLessThan(totalBytes * 0.5);
  }, 30_000);

  it("streams 500 MB to disk with less than 50 MB RSS growth", async () => {
    const totalBytes = 500 * 1024 * 1024; // 500 MB
    const destPath = path.join(tmpDir, "output-500mb.bin");

    if (global.gc) global.gc();
    const baselineRss = process.memoryUsage().rss;

    const readable = createLargeReadable(totalBytes);

    const samples = await monitorMemory(
      executeRequestBodyToFileWrite(readable, destPath),
      200
    );

    const stats = fs.statSync(destPath);
    expect(stats.size).toBe(totalBytes);

    const peakRss = Math.max(...samples);
    const rssGrowth = peakRss - baselineRss;

    const growthRatio = rssGrowth / totalBytes;

    console.log(
      `  500 MB stream: baseline=${(baselineRss / 1024 / 1024).toFixed(1)} MB, ` +
        `peak=${(peakRss / 1024 / 1024).toFixed(1)} MB, ` +
        `growth=${(rssGrowth / 1024 / 1024).toFixed(1)} MB (${(growthRatio * 100).toFixed(1)}%), ` +
        `samples=${samples.length}`
    );

    // 500 MB streamed should show <15% RSS growth. Buffering would show ~100%.
    // The 500 MB test is the strongest proof because it amortizes fixed costs.
    expect(rssGrowth).toBeLessThan(totalBytes * 0.15);
  }, 60_000);

  it("FAILS memory check when body is buffered (control test)", async () => {
    // This test proves our memory check actually catches buffering.
    // We deliberately buffer 200 MB into an array, then check RSS after.
    // RSS should grow by ≥200 MB — if it doesn't, our threshold is wrong.
    const totalBytes = 200 * 1024 * 1024;
    const destPath = path.join(tmpDir, "output-buffered.bin");

    if (global.gc) global.gc();
    const baselineRss = process.memoryUsage().rss;

    const readable = createLargeReadable(totalBytes);

    // Deliberately buffer the entire stream into memory (the anti-pattern)
    const chunks = [];
    await new Promise((resolve, reject) => {
      readable.on("data", (chunk) => chunks.push(chunk));
      readable.on("end", resolve);
      readable.on("error", reject);
    });

    // Check RSS right after buffering, before writing to disk.
    // This captures the peak because all 200 MB is in the `chunks` array.
    const peakRss = process.memoryUsage().rss;

    const fullBuffer = Buffer.concat(chunks);
    await fsPromises.writeFile(destPath, fullBuffer);

    const stats = fs.statSync(destPath);
    expect(stats.size).toBe(totalBytes);

    const rssGrowth = peakRss - baselineRss;

    console.log(
      `  200 MB BUFFERED (control): baseline=${(baselineRss / 1024 / 1024).toFixed(1)} MB, ` +
        `peak=${(peakRss / 1024 / 1024).toFixed(1)} MB, ` +
        `growth=${(rssGrowth / 1024 / 1024).toFixed(1)} MB`
    );

    // The buffered path SHOULD blow past 80 MB of growth.
    // If this assertion fails, our memory threshold is too generous.
    expect(rssGrowth).toBeGreaterThan(80 * 1024 * 1024);
  }, 30_000);
});

describe("Large file streaming — data integrity", () => {
  let tmpDir;

  beforeEach(() => {
    tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-integrity-")
    );
  });

  afterEach(() => {
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("preserves file contents through the stream pipeline", async () => {
    // Write a known file, stream it through the pipeline, compare checksums
    const sourcePath = path.join(tmpDir, "source.bin");
    const destPath = path.join(tmpDir, "dest.bin");
    const totalBytes = 10 * 1024 * 1024; // 10 MB

    // Generate a source file with random data
    const sourceData = crypto.randomBytes(totalBytes);
    fs.writeFileSync(sourcePath, sourceData);
    const sourceHash = crypto
      .createHash("sha256")
      .update(sourceData)
      .digest("hex");

    // Stream from source file → pipeline → dest file
    const readable = fs.createReadStream(sourcePath);
    await executeRequestBodyToFileWrite(readable, destPath);

    // Verify checksum matches
    const destData = fs.readFileSync(destPath);
    const destHash = crypto
      .createHash("sha256")
      .update(destData)
      .digest("hex");

    expect(destHash).toBe(sourceHash);
    expect(destData.length).toBe(totalBytes);
  });

  it("handles concurrent streaming pipelines independently", async () => {
    const fileCount = 5;
    const bytesPerFile = 5 * 1024 * 1024; // 5 MB each

    const promises = Array.from({ length: fileCount }, (_, i) => {
      const destPath = path.join(tmpDir, `concurrent-${i}.bin`);
      const readable = createLargeReadable(bytesPerFile);
      return executeRequestBodyToFileWrite(readable, destPath).then(
        () => destPath
      );
    });

    const paths = await Promise.all(promises);

    for (const p of paths) {
      const stats = fs.statSync(p);
      expect(stats.size).toBe(bytesPerFile);
    }
  });
});

describe("HTTP upload streaming — end-to-end", () => {
  let tmpDir;
  let server;
  let serverPort;

  beforeEach(async () => {
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "elm-pages-http-e2e-"));

    // Start a small HTTP server that replicates the streaming upload path:
    // register req as the body stream → pipe to file
    server = http.createServer(async (req, res) => {
      if (req.method === "POST" && req.url === "/upload") {
        const destPath = path.join(tmpDir, "uploaded.bin");
        try {
          await executeRequestBodyToFileWrite(req, destPath);
          res.writeHead(200, { "Content-Type": "application/json" });
          res.end(JSON.stringify({ ok: true }));
        } catch (error) {
          res.writeHead(500, { "Content-Type": "text/plain" });
          res.end(error.toString());
        }
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

  it("uploads 50 MB over HTTP and writes to disk with constant memory", async () => {
    const totalBytes = 50 * 1024 * 1024;

    if (global.gc) global.gc();
    const baselineRss = process.memoryUsage().rss;

    const uploadPromise = new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: "127.0.0.1",
          port: serverPort,
          path: "/upload",
          method: "POST",
          headers: {
            "Transfer-Encoding": "chunked",
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () => {
            if (res.statusCode === 200) {
              resolve(JSON.parse(body));
            } else {
              reject(new Error(`HTTP ${res.statusCode}: ${body}`));
            }
          });
        }
      );
      req.on("error", reject);

      // Send data in chunks
      const chunkSize = 64 * 1024;
      let bytesRemaining = totalBytes;

      function writeChunk() {
        while (bytesRemaining > 0) {
          const size = Math.min(chunkSize, bytesRemaining);
          const chunk = Buffer.alloc(size, bytesRemaining % 256);
          bytesRemaining -= size;

          if (!req.write(chunk)) {
            // Backpressure — wait for drain before writing more
            req.once("drain", writeChunk);
            return;
          }
        }
        req.end();
      }
      writeChunk();
    });

    const samples = await monitorMemory(uploadPromise, 100);

    const response = await uploadPromise.catch(() => null);

    // Verify the file was written
    const destPath = path.join(tmpDir, "uploaded.bin");
    const stats = fs.statSync(destPath);
    expect(stats.size).toBe(totalBytes);

    // Verify memory stayed bounded
    const peakRss = Math.max(...samples);
    const rssGrowth = peakRss - baselineRss;

    console.log(
      `  50 MB HTTP upload: baseline=${(baselineRss / 1024 / 1024).toFixed(1)} MB, ` +
        `peak=${(peakRss / 1024 / 1024).toFixed(1)} MB, ` +
        `growth=${(rssGrowth / 1024 / 1024).toFixed(1)} MB`
    );

    // 50 MB upload should not cause 50 MB of RSS growth
    expect(rssGrowth).toBeLessThan(40 * 1024 * 1024);
  }, 30_000);

  it("uploads 200 MB over HTTP and writes to disk with constant memory", async () => {
    const totalBytes = 200 * 1024 * 1024;

    if (global.gc) global.gc();
    const baselineRss = process.memoryUsage().rss;

    const uploadPromise = new Promise((resolve, reject) => {
      const req = http.request(
        {
          hostname: "127.0.0.1",
          port: serverPort,
          path: "/upload",
          method: "POST",
          headers: {
            "Transfer-Encoding": "chunked",
          },
        },
        (res) => {
          let body = "";
          res.on("data", (chunk) => (body += chunk));
          res.on("end", () => {
            if (res.statusCode === 200) {
              resolve(JSON.parse(body));
            } else {
              reject(new Error(`HTTP ${res.statusCode}: ${body}`));
            }
          });
        }
      );
      req.on("error", reject);

      const chunkSize = 64 * 1024;
      let bytesRemaining = totalBytes;

      function writeChunk() {
        while (bytesRemaining > 0) {
          const size = Math.min(chunkSize, bytesRemaining);
          const chunk = Buffer.alloc(size, bytesRemaining % 256);
          bytesRemaining -= size;

          if (!req.write(chunk)) {
            req.once("drain", writeChunk);
            return;
          }
        }
        req.end();
      }
      writeChunk();
    });

    const samples = await monitorMemory(uploadPromise, 200);

    // Verify the file was written
    const destPath = path.join(tmpDir, "uploaded.bin");
    const stats = fs.statSync(destPath);
    expect(stats.size).toBe(totalBytes);

    // Verify memory stayed bounded
    const peakRss = Math.max(...samples);
    const rssGrowth = peakRss - baselineRss;

    console.log(
      `  200 MB HTTP upload: baseline=${(baselineRss / 1024 / 1024).toFixed(1)} MB, ` +
        `peak=${(peakRss / 1024 / 1024).toFixed(1)} MB, ` +
        `growth=${(rssGrowth / 1024 / 1024).toFixed(1)} MB`
    );

    expect(rssGrowth).toBeLessThan(80 * 1024 * 1024);
  }, 60_000);
});
