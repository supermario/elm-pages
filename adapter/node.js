import * as fs from "fs";
import * as path from "path";

/**
 * Node.js HTTP server adapter for elm-pages.
 *
 * Generates a standalone Node.js HTTP server that supports streaming request
 * bodies and streaming responses. This is the recommended adapter for
 * production use when handling large file uploads or downloads, since
 * serverless adapters (like Netlify) buffer the entire request body before
 * your code runs.
 *
 * The generated server:
 * - Serves pre-rendered static files from `dist/`
 * - Routes server-rendered pages and API routes through elm-pages
 * - Supports streaming uploads and responses for serverRenderStreaming routes
 * - Automatically buffers request bodies for serverRender routes via readBody BackendTask
 */
export default async function run({
  renderFunctionFilePath,
  routePatterns,
  apiRoutePatterns,
}) {
  console.log("Running Node.js server adapter");
  ensureDirSync("server");

  fs.copyFileSync(renderFunctionFilePath, "./server/elm-pages-cli.mjs");
  fs.writeFileSync("./server/index.mjs", serverCode());
  console.log(
    'Node.js server generated at server/index.mjs. Start with: node server/index.mjs'
  );
}

function serverCode() {
  return `import * as http from "node:http";
import * as path from "node:path";
import * as fs from "node:fs";
import * as url from "node:url";
import { randomUUID } from "node:crypto";
import { Readable } from "node:stream";
import * as elmPages from "./elm-pages-cli.mjs";
import * as busboy from "busboy";

const PORT = process.env.PORT || 3000;
const HOST = process.env.HOST || "0.0.0.0";
const DIST_DIR = path.join(process.cwd(), "dist");

const server = http.createServer(async (req, res) => {
  const parsedUrl = new URL(req.url, \`http://\${req.headers.host || "localhost"}\`);
  const pathname = parsedUrl.pathname;

  // Try to serve static files from dist/ first
  const staticFilePath = path.join(DIST_DIR, pathname === "/" ? "index.html" : pathname);
  if (fs.existsSync(staticFilePath) && fs.statSync(staticFilePath).isFile()) {
    const ext = path.extname(staticFilePath).toLowerCase();
    const mimeTypes = {
      ".html": "text/html", ".js": "application/javascript", ".css": "text/css",
      ".json": "application/json", ".png": "image/png", ".jpg": "image/jpeg",
      ".gif": "image/gif", ".svg": "image/svg+xml", ".ico": "image/x-icon",
      ".xml": "application/xml", ".txt": "text/plain", ".woff": "font/woff",
      ".woff2": "font/woff2",
    };
    res.writeHead(200, { "Content-Type": mimeTypes[ext] || "application/octet-stream" });
    fs.createReadStream(staticFilePath).pipe(res);
    return;
  }

  // Route through elm-pages
  const hasBody = ["POST", "PUT", "PATCH"].includes(req.method);

  if (hasBody) {
    // For requests with a body, register the request stream so it's available
    // to both serverRender (auto-read via readBody BackendTask) and
    // serverRenderStreaming (piped via Stream.requestBody).
    const requestId = randomUUID();
    elmPages.registerRequestBodyStream(requestId, req);
    elmPages.setCurrentRequestId(requestId);
    req.setTimeout(0);

    const serverRequest = buildRequest(req, null);

    try {
      const renderResult = await elmPages.render(serverRequest);
      await sendResponse(renderResult, res, pathname);
    } catch (error) {
      console.error("Render error:", error);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" });
      }
      res.end("Internal Server Error");
    } finally {
      elmPages.unregisterRequestBodyStream(requestId);
      elmPages.setCurrentRequestId(null);
    }
  } else {
    // GET, HEAD, DELETE, OPTIONS — no body to stream.
    const serverRequest = buildRequest(req, null);

    try {
      const renderResult = await elmPages.render(serverRequest);
      await sendResponse(renderResult, res, pathname);
    } catch (error) {
      console.error("Render error:", error);
      if (!res.headersSent) {
        res.writeHead(500, { "Content-Type": "text/plain" });
      }
      res.end("Internal Server Error");
    }
  }
});

/**
 * Build a request JSON object for elm-pages.
 */
function buildRequest(req, body) {
  const parsedUrl = new URL(req.url, \`http://\${req.headers.host || "localhost"}\`);
  return {
    requestTime: Math.round(new Date().getTime()),
    method: req.method,
    headers: req.headers || {},
    rawUrl: parsedUrl.toString(),
    body: body,
    multiPartFormData: null,
  };
}

/**
 * Build a request JSON with multipart form data parsing if applicable.
 */
async function buildRequestWithMultipart(req, body) {
  if (
    req.method === "POST" &&
    req.headers["content-type"] &&
    req.headers["content-type"].includes("multipart/form-data") &&
    body
  ) {
    return new Promise((resolve) => {
      try {
        const bb = busboy.default({ headers: req.headers });
        const fields = {};

        bb.on("file", (fieldname, file, info) => {
          const { filename, mimeType } = info;
          file.on("data", (data) => {
            fields[fieldname] = { filename, mimeType, body: data.toString() };
          });
        });

        bb.on("field", (fieldName, value) => {
          fields[fieldName] = value;
        });

        bb.on("close", () => {
          resolve(Object.assign(buildRequest(req, body), { multiPartFormData: fields }));
        });

        bb.write(body);
      } catch (error) {
        resolve(buildRequest(req, body));
      }
    });
  }
  return buildRequest(req, body);
}

/**
 * Send the render result as an HTTP response.
 */
async function sendResponse(renderResult, res, pathname) {
  if (renderResult.kind === "api-response") {
    const response = renderResult.body;
    if (response.kind === "server-response") {
      setHeaders(res, response.headers);
      res.writeHead(response.statusCode);
      if (response.isBase64Encoded && response.body) {
        res.end(Buffer.from(response.body, "base64"));
      } else {
        res.end(response.body);
      }
    } else if (response.kind === "streaming-server-response") {
      const result = await elmPages.executeStreamToResponse(
        response.streamPipeline,
        res,
        {},
        {
          statusCode: response.statusCode,
          headers: response.headers,
        }
      );
      if (!result.ok) {
        res.writeHead(500, { "Content-Type": "text/plain" });
        res.end("Streaming response error: " + result.error);
      }
    } else if (response.kind === "static-file") {
      res.writeHead(renderResult.statusCode, { "Content-Type": "text/html" });
      res.end(response.body);
    }
  } else if (renderResult.kind === "html") {
    res.writeHead(renderResult.statusCode, {
      "Content-Type": "text/html",
      ...flattenHeaders(renderResult.headers),
    });
    res.end(renderResult.body);
  } else if (renderResult.kind === "bytes") {
    res.writeHead(renderResult.statusCode, {
      "Content-Type": "application/octet-stream",
      ...flattenHeaders(renderResult.headers),
    });
    res.end(Buffer.from(renderResult.body, "base64"));
  } else {
    res.writeHead(500, { "Content-Type": "text/plain" });
    res.end("Unknown render result kind: " + renderResult.kind);
  }
}

function setHeaders(res, headers) {
  if (!headers) return;
  for (const [key, values] of Object.entries(headers)) {
    if (Array.isArray(values)) {
      values.forEach((value) => res.setHeader(key, value));
    } else {
      res.setHeader(key, values);
    }
  }
}

function flattenHeaders(headers) {
  if (!headers) return {};
  const result = {};
  for (const [key, values] of Object.entries(headers)) {
    result[key] = Array.isArray(values) ? values.join(", ") : values;
  }
  return result;
}

server.listen(PORT, HOST, () => {
  console.log(\`elm-pages server listening on http://\${HOST}:\${PORT}\`);
});
`;
}

/**
 * @param {string} dirpath
 */
function ensureDirSync(dirpath) {
  try {
    fs.mkdirSync(dirpath, { recursive: true });
  } catch (err) {
    if (err.code !== "EEXIST") throw err;
  }
}
