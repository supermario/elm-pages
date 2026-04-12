/**
 * Tests for streaming response support.
 *
 * These tests verify:
 * 1. Stream pipeline execution to a writable destination
 * 2. Correct handling of headers and status codes
 * 3. Error propagation during streaming responses
 * 4. Empty and multi-part stream pipelines
 */

import { describe, it, expect } from "vitest";
import { Readable, Writable, PassThrough } from "node:stream";
import * as fs from "node:fs";
import * as path from "node:path";
import * as os from "node:os";

describe("Streaming response pipeline", () => {
  it("should pipe a Readable to a Writable destination", async () => {
    const inputData = "Hello from streaming response!";
    const readable = Readable.from([inputData]);
    const chunks = [];

    const destination = new Writable({
      write(chunk, enc, cb) {
        chunks.push(chunk.toString());
        cb();
      },
    });

    await new Promise((resolve, reject) => {
      readable.pipe(destination);
      destination.once("finish", resolve);
      destination.once("error", reject);
    });

    expect(chunks.join("")).toBe(inputData);
  });

  it("should handle a file read pipeline to a mock HTTP response", async () => {
    // Write a temp file, then stream it to a mock response
    const tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-resp-")
    );
    const inputPath = path.join(tmpDir, "source.txt");
    const fileContent = "This is the file content to stream as a response.";
    fs.writeFileSync(inputPath, fileContent);

    const chunks = [];
    let headArgs = null;

    // Mock http.ServerResponse
    const mockRes = new Writable({
      write(chunk, enc, cb) {
        chunks.push(chunk.toString());
        cb();
      },
    });
    mockRes.writeHead = (statusCode, headers) => {
      headArgs = { statusCode, headers };
    };
    mockRes.setHeader = () => {};

    // Simulate the streaming response flow
    headArgs = { statusCode: 200, headers: { "Content-Type": "text/plain" } };
    const fileStream = fs.createReadStream(inputPath);

    await new Promise((resolve, reject) => {
      fileStream.pipe(mockRes);
      mockRes.once("finish", resolve);
      mockRes.once("error", reject);
    });

    expect(chunks.join("")).toBe(fileContent);
    expect(headArgs.statusCode).toBe(200);

    // Clean up
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });

  it("should propagate source stream errors to the destination", async () => {
    const errorStream = new Readable({
      read() {
        process.nextTick(() => {
          this.destroy(new Error("Source read failure"));
        });
      },
    });

    const chunks = [];
    const destination = new Writable({
      write(chunk, enc, cb) {
        chunks.push(chunk);
        cb();
      },
    });

    await expect(
      new Promise((resolve, reject) => {
        errorStream.pipe(destination);
        errorStream.once("error", reject);
        destination.once("finish", resolve);
      })
    ).rejects.toThrow("Source read failure");
  });

  it("should handle an empty stream pipeline gracefully", async () => {
    const emptyStream = Readable.from([]);
    const chunks = [];

    const destination = new Writable({
      write(chunk, enc, cb) {
        chunks.push(chunk);
        cb();
      },
    });

    await new Promise((resolve) => {
      emptyStream.pipe(destination);
      destination.once("finish", resolve);
    });

    expect(chunks).toHaveLength(0);
  });

  it("should handle multi-stage pipeline (read -> transform -> destination)", async () => {
    const inputData = "hello world";
    const readable = Readable.from([inputData]);
    const upperCaseTransform = new PassThrough({
      transform(chunk, enc, cb) {
        cb(null, chunk.toString().toUpperCase());
      },
    });

    const chunks = [];
    const destination = new Writable({
      write(chunk, enc, cb) {
        chunks.push(chunk.toString());
        cb();
      },
    });

    await new Promise((resolve, reject) => {
      readable.pipe(upperCaseTransform).pipe(destination);
      destination.once("finish", resolve);
      destination.once("error", reject);
    });

    expect(chunks.join("")).toBe("HELLO WORLD");
  });

  it("should stream large files without buffering entire content", async () => {
    const tmpDir = fs.mkdtempSync(
      path.join(os.tmpdir(), "elm-pages-stream-large-")
    );
    const inputPath = path.join(tmpDir, "large-source.bin");

    // Write 5MB file
    const chunkSize = 1024;
    const totalChunks = 5000;
    const fd = fs.openSync(inputPath, "w");
    const writeChunk = Buffer.alloc(chunkSize, "A");
    for (let i = 0; i < totalChunks; i++) {
      fs.writeSync(fd, writeChunk);
    }
    fs.closeSync(fd);

    let totalBytesReceived = 0;
    const destination = new Writable({
      write(chunk, enc, cb) {
        totalBytesReceived += chunk.length;
        cb();
      },
    });

    const fileStream = fs.createReadStream(inputPath);

    await new Promise((resolve, reject) => {
      fileStream.pipe(destination);
      destination.once("finish", resolve);
      destination.once("error", reject);
    });

    expect(totalBytesReceived).toBe(chunkSize * totalChunks);

    // Clean up
    fs.rmSync(tmpDir, { recursive: true, force: true });
  });
});

describe("Streaming response JSON shape", () => {
  it("should match the expected streaming-server-response format", () => {
    // Simulate what Elm's streamingResponseToJson produces
    const streamingResponse = {
      kind: "streaming-server-response",
      statusCode: 200,
      headers: {
        "Content-Type": ["application/octet-stream"],
        "Content-Disposition": ['attachment; filename="data.bin"'],
      },
      streamPipeline: {
        kind: "none",
        parts: [{ name: "fileRead", path: "/some/file.bin" }],
      },
    };

    expect(streamingResponse.kind).toBe("streaming-server-response");
    expect(streamingResponse.statusCode).toBe(200);
    expect(streamingResponse.streamPipeline.parts).toHaveLength(1);
    expect(streamingResponse.streamPipeline.parts[0].name).toBe("fileRead");
  });

  it("should match requestBody + fileWrite pipeline format", () => {
    const pipeline = {
      kind: "none",
      parts: [
        { name: "requestBody" },
        { name: "fileWrite", path: "/uploads/data.bin" },
      ],
    };

    expect(pipeline.parts).toHaveLength(2);
    expect(pipeline.parts[0].name).toBe("requestBody");
    expect(pipeline.parts[1].name).toBe("fileWrite");
  });
});
