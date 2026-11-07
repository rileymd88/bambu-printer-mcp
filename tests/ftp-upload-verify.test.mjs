import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import fs from "node:fs/promises";
import net from "node:net";
import os from "node:os";
import path from "node:path";
import { Readable } from "node:stream";
import tls from "node:tls";
import { test } from "node:test";
import { Client } from "basic-ftp";
import {
  incompleteUpload, measureRemoteFile, storeVerifiedBytes, UploadTransferError,
} from "../dist/printers/ftp-upload.js";

// The sanctioned A1 mini plate from the Nov 7 upload failure. The bytes are the archive; the .3mf name is the real type.
const PLATE_URL = new URL("./fixtures/a1mini-carousel-plate1.3mf", import.meta.url);
const PLATE_SHA256 = "cb2e1043b52af8c180211141c06b3083dbdc6b22c19444bf36ba7b40af9457c4";
const PLATE_LENGTH = 4_847_245;
const ACCESS_CODE = "ACCESS-CODE-12345678";

async function plateBytes() {
  const bytes = await fs.readFile(PLATE_URL);
  assert.equal(bytes.length, PLATE_LENGTH);
  assert.equal(createHash("sha256").update(bytes).digest("hex"), PLATE_SHA256);
  return bytes;
}

function listen(server) {
  return new Promise((resolve, reject) => {
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => resolve(server.address().port));
  });
}

/**
 * Passive FTP (or implicit FTPS) that can pause like a slow card and answer SIZE.
 * Data connections are a separate socket. When `secure` is set, that socket is TLS too.
 */
async function printerFtp(t, { secure, stall = false, pauseAt = Infinity, pauseMs = 0, rate = 8 * 1024 * 1024, confirm = true } = {}) {
  const files = new Map();
  const facts = [];
  const sockets = new Set();
  const track = socket => {
    sockets.add(socket);
    socket.on("close", () => sockets.delete(socket));
    socket.on("error", () => {});
  };
  const make = handler => secure ? tls.createServer(secure, handler) : net.createServer(handler);
  const queued = [];
  const waiters = [];
  const incoming = new Map();
  const deliver = socket => {
    facts.push({
      encrypted: socket.encrypted === true,
      protocol: typeof socket.getProtocol === "function" ? socket.getProtocol() : undefined,
      reused: typeof socket.isSessionReused === "function" && socket.isSessionReused(),
    });
    // Collect before STOR runs. A timeout can destroy the socket before that command is handled.
    const bucket = { parts: [], taken: 0, paused: false };
    incoming.set(socket, bucket);
    socket.on("data", chunk => {
      bucket.parts.push(chunk);
      bucket.taken += chunk.length;
      socket.pause();
      // A paused socket does not emit end/close when the client gives up, so remember the partial now.
      if (stall) { bucket.stalled = true; bucket.onStall?.(); return; }
      const extra = !bucket.paused && bucket.taken >= pauseAt ? (bucket.paused = true, pauseMs) : 0;
      setTimeout(() => socket.resume(), Math.ceil(chunk.length / rate * 1000) + extra);
    });
    const waiter = waiters.shift();
    if (waiter) waiter(socket);
    else queued.push(socket);
  };
  const nextSocket = () => {
    const socket = queued.shift();
    if (socket) return Promise.resolve(socket);
    return new Promise(resolve => waiters.push(resolve));
  };
  const readUpload = socket => new Promise(resolve => {
    const bucket = incoming.get(socket) ?? { parts: [] };
    let done = false;
    const finish = () => { if (!done) { done = true; resolve(Buffer.concat(bucket.parts)); } };
    if (bucket.stalled || socket.destroyed) finish();
    else {
      bucket.onStall = finish;
      socket.on("end", finish);
      socket.on("close", finish);
    }
  });
  const data = make(socket => { track(socket); deliver(socket); });
  const control = make(socket => {
    track(socket);
    socket.write("220 ready\r\n");
    let buffer = "";
    let chain = Promise.resolve();
    const command = async line => {
      const space = line.indexOf(" ");
      const verb = (space === -1 ? line : line.slice(0, space)).toUpperCase();
      const arg = space === -1 ? "" : line.slice(space + 1);
      const name = arg.replace(/^\/+/, "");
      if (verb === "USER") socket.write("331 \r\n");
      else if (verb === "PASS") socket.write("230 \r\n");
      else if (verb === "PASV") {
        const port = data.address().port;
        socket.write(`227 (127,0,0,1,${port >> 8},${port & 255})\r\n`);
      } else if (verb === "EPSV" || verb === "EPRT") socket.write("502 \r\n");
      else if (verb === "FEAT") socket.write("211 End\r\n");
      else if (verb === "STOR") {
        socket.write("150 \r\n");
        const dataSocket = await nextSocket();
        const body = await readUpload(dataSocket);
        files.set(name, body);
        if (confirm && !stall) socket.write("226 \r\n");
      } else if (verb === "LIST" || verb === "NLST") {
        socket.write("150 \r\n");
        const dataSocket = await nextSocket();
        const listing = [...files.entries()].map(([file, body]) => `-rw-r--r-- 1 o g ${body.length} Jan 1 00:00 ${path.posix.basename(file)}\r\n`).join("");
        await new Promise(resolve => dataSocket.end(listing, resolve));
        socket.write("226 \r\n");
      } else if (verb === "SIZE") {
        const body = files.get(name);
        socket.write(body ? `213 ${body.length}\r\n` : "550 missing\r\n");
      } else socket.write("200 \r\n");
    };
    socket.on("data", chunk => {
      buffer += chunk;
      let end;
      while ((end = buffer.indexOf("\r\n")) >= 0) {
        const line = buffer.slice(0, end);
        buffer = buffer.slice(end + 2);
        chain = chain.then(() => command(line)).catch(() => { try { socket.write("500 \r\n"); } catch { /* the client already left */ } });
      }
    });
  });
  const dataPort = await listen(data);
  const port = await listen(control);
  t.after(async () => {
    for (const socket of sockets) socket.destroy();
    await Promise.all([control, data].map(server => new Promise(resolve => server.close(resolve))));
  });
  return { port, files, facts, dataPort };
}

async function access(port, secure) {
  const client = new Client(15_000);
  await client.access({
    host: "127.0.0.1", port, user: "bblp", password: ACCESS_CODE,
    ...(secure ? { secure: "implicit", secureOptions: { rejectUnauthorized: false, host: "127.0.0.1" } } : { secure: false }),
  });
  return client;
}

test("a matching stored size does not make a failed transfer printable", async () => {
  const failed = new UploadTransferError(new Error(`Timeout (data socket) ${ACCESS_CODE}`), {
    stage: "store", bytesSent: 50, bytesTotal: 50, remotePath: "/cache/checked-plate.3mf", secrets: [ACCESS_CODE],
  });
  const reported = await incompleteUpload(failed, async () => ({ bytes: 50 }), [ACCESS_CODE]);
  assert.equal(reported.stage, "store");
  assert.match(reported.message, /store: 50 of 50 bytes were handed to the data socket for cache\/checked-plate\.3mf/);
  assert.match(reported.message, /reports 50 stored bytes/);
  assert.match(reported.message, /must not be printed/);
  assert.match(reported.message, /Timeout \(data socket\)/);
  assert.equal(reported.message.includes(ACCESS_CODE), false);
  assert.equal(reported.logLine.startsWith("ftps upload: "), true);
  assert.equal(reported.logLine.includes(ACCESS_CODE), false);
});

test("one write of the A1 mini plate hits the data-socket timeout while bytes are still queued", { timeout: 30_000 }, async t => {
  // 1.1.69 handed basic-ftp the whole buffer. A pause after the first accepted block, with the rest still queued, is the stall.
  const bytes = await plateBytes();
  const printer = await printerFtp(t, { pauseAt: 64 * 1024, pauseMs: 2_000, rate: 32 * 1024 * 1024 });
  const client = new Client(1_000);
  t.after(() => client.close());
  await client.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
  await assert.rejects(client.uploadFrom(Readable.from(bytes), "/cache/checked-plate.3mf"), /Timeout \(data socket\)/);
});

test("the A1 mini plate uploads in chunks, survives a pause past the command timeout, and matches SIZE", { timeout: 30_000 }, async t => {
  const bytes = await plateBytes();
  const printer = await printerFtp(t, { pauseAt: 64 * 1024, pauseMs: 2_000, rate: 32 * 1024 * 1024 });
  const client = new Client(1_000);
  t.after(() => client.close());
  await client.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
  await storeVerifiedBytes(client, bytes, "/cache/checked-plate.3mf", { stallMs: 8_000 });
  const stored = printer.files.get("cache/checked-plate.3mf");
  assert.ok(stored.equals(bytes), "the printer keeps the exact inspected bytes");
  assert.equal(await client.size("/cache/checked-plate.3mf"), bytes.length);
});

test("a stalled plate leaves a short file, reports both byte counts, and is not a successful upload", { timeout: 30_000 }, async t => {
  const bytes = await plateBytes();
  const printer = await printerFtp(t, { stall: true, rate: 32 * 1024 * 1024 });
  const client = new Client(1_000);
  t.after(() => client.close());
  await client.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
  const error = await storeVerifiedBytes(client, bytes, "/cache/checked-plate.3mf", { stallMs: 1_500 }).then(() => {
    throw new Error("stalled upload returned");
  }, caught => caught);
  assert.ok(error instanceof UploadTransferError);
  assert.equal(error.stage, "store");
  assert.match(error.message, /Timeout \(data socket\)/);
  assert.ok(error.bytesSent < bytes.length, `handed ${error.bytesSent}, file is ${bytes.length}`);
  const storedName = "cache/checked-plate.3mf";
  const deadline = Date.now() + 2_000;
  while (!printer.files.has(storedName) && Date.now() < deadline) await new Promise(resolve => setTimeout(resolve, 20));
  const stored = printer.files.get(storedName);
  assert.ok(stored && stored.length > 0 && stored.length < bytes.length);
  const probe = new Client(2_000);
  t.after(() => probe.close());
  const measured = await measureRemoteFile(async () => {
    await probe.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
    return probe;
  }, "/cache/checked-plate.3mf", [ACCESS_CODE]);
  assert.equal(measured.bytes, stored.length);
  const reported = await incompleteUpload(error, async () => measured, [ACCESS_CODE]);
  assert.match(reported.message, new RegExp(`reports ${stored.length} stored bytes`));
  assert.match(reported.message, /must not be printed/);
  assert.equal(reported.message.includes(ACCESS_CODE), false);
  assert.ok(!stored.equals(bytes));
});

test("a full stored file is still refused when the printer never confirms the transfer", async t => {
  const bytes = Buffer.from("exact-bytes-without-226");
  const printer = await printerFtp(t, { confirm: false });
  const client = new Client(400);
  t.after(() => client.close());
  await client.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
  const error = await storeVerifiedBytes(client, bytes, "/cache/quiet.3mf", { stallMs: 400 }).then(() => {
    throw new Error("unconfirmed upload returned");
  }, caught => caught);
  assert.ok(error instanceof UploadTransferError);
  assert.match(error.message, /Timeout/);
  assert.equal(printer.files.get("cache/quiet.3mf").equals(bytes), true);
  const reported = await incompleteUpload(error, () => measureRemoteFile(async () => {
    const probe = new Client(2_000);
    await probe.access({ host: "127.0.0.1", port: printer.port, user: "bblp", password: ACCESS_CODE, secure: false });
    return probe;
  }, "/cache/quiet.3mf", [ACCESS_CODE]), [ACCESS_CODE]);
  assert.match(reported.message, new RegExp(`reports ${bytes.length} stored bytes`));
  assert.match(reported.message, /must not be printed/);
  assert.equal(reported.message.includes(ACCESS_CODE), false);
});

test("implicit FTPS keeps the data channel encrypted and resumes the control session", { timeout: 20_000 }, async t => {
  const dir = await fs.mkdtemp(path.join(os.tmpdir(), "bambu-ftps-"));
  t.after(() => fs.rm(dir, { recursive: true, force: true }));
  const keyPath = path.join(dir, "key.pem");
  const certPath = path.join(dir, "cert.pem");
  execFileSync("openssl", ["req", "-x509", "-newkey", "rsa:2048", "-keyout", keyPath, "-out", certPath, "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1"], { stdio: "ignore" });
  const secure = { key: await fs.readFile(keyPath), cert: await fs.readFile(certPath), ticketKeys: randomBytes(48) };
  const bytes = Buffer.from("encrypted-a1-mini-upload");
  const printer = await printerFtp(t, { secure });
  const client = await access(printer.port, secure);
  t.after(() => client.close());
  const listing = await client.list("/");
  assert.equal(listing.length, 0);
  await storeVerifiedBytes(client, bytes, "/cache/checked-tls.3mf", { stallMs: 5_000 });
  assert.ok(printer.files.get("cache/checked-tls.3mf").equals(bytes));
  assert.ok(printer.facts.length >= 2, "directory listing and STOR each open a data connection");
  for (const fact of printer.facts) {
    assert.equal(fact.encrypted, true);
    assert.match(fact.protocol ?? "", /^TLS/);
    assert.equal(fact.reused, true, "the data connection resumes the control session instead of negotiating in the clear");
  }
});
