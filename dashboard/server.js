const http = require("node:http");
const fs = require("node:fs");
const path = require("node:path");

const PORT = Number(process.env.PORT || 3000);
const HOST = process.env.HOST || "127.0.0.1";
const API_BASE_URL = (process.env.API_BASE_URL || "http://127.0.0.1:8000").replace(/\/$/, "");
const PUBLIC_DIR = path.join(__dirname, "public");

const MIME_TYPES = {
  ".html": "text/html; charset=utf-8",
  ".js": "application/javascript; charset=utf-8",
  ".css": "text/css; charset=utf-8",
  ".json": "application/json; charset=utf-8",
};

function sendJson(res, statusCode, payload) {
  const body = Buffer.from(JSON.stringify(payload), "utf-8");
  res.writeHead(statusCode, {
    "Content-Type": "application/json; charset=utf-8",
    "Content-Length": body.length,
    "Access-Control-Allow-Origin": "*",
  });
  res.end(body);
}

function proxyToSink(reqUrl, res) {
  const target = new URL(reqUrl, API_BASE_URL);
  http
    .get(target, (upstream) => {
      const chunks = [];
      upstream.on("data", (chunk) => chunks.push(chunk));
      upstream.on("end", () => {
        const body = Buffer.concat(chunks);
        res.writeHead(upstream.statusCode || 200, {
          "Content-Type": upstream.headers["content-type"] || "application/json; charset=utf-8",
          "Content-Length": body.length,
          "Access-Control-Allow-Origin": "*",
        });
        res.end(body);
      });
    })
    .on("error", (err) => {
      sendJson(res, 502, {
        error: "Telemetry REST bridge unreachable",
        target: target.toString(),
        detail: err.message,
      });
    });
}

const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url || "/", `http://${req.headers.host || "127.0.0.1"}`);
  if (parsedUrl.pathname === "/health" || parsedUrl.pathname.startsWith("/api/")) {
    proxyToSink(parsedUrl.pathname + parsedUrl.search, res);
    return;
  }

  const relPath = parsedUrl.pathname === "/" ? "index.html" : parsedUrl.pathname.replace(/^\/+/, "");
  const filePath = path.normalize(path.join(PUBLIC_DIR, relPath));
  if (!filePath.startsWith(PUBLIC_DIR) || !fs.existsSync(filePath) || fs.statSync(filePath).isDirectory()) {
    sendJson(res, 404, { error: "Not Found", path: parsedUrl.pathname });
    return;
  }

  const ext = path.extname(filePath).toLowerCase();
  const data = fs.readFileSync(filePath);
  res.writeHead(200, {
    "Content-Type": MIME_TYPES[ext] || "application/octet-stream",
    "Content-Length": data.length,
  });
  res.end(data);
});

server.listen(PORT, HOST, () => {
  console.log(`[ATMOSYNC DASHBOARD] UI=http://${HOST}:${PORT} -> API=${API_BASE_URL}`);
});
