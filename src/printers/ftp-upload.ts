import { Readable } from "node:stream";
import type { Client as FTPClient } from "basic-ftp";

/** Bytes per write on the FTPS data socket; matches fs.createReadStream's default. */
export const FTP_UPLOAD_CHUNK_BYTES = 64 * 1024;

function* chunks(bytes: Buffer, size: number): Generator<Buffer> {
  for (let offset = 0; offset < bytes.length; offset += size) yield bytes.subarray(offset, offset + size);
}

/**
 * How long a print upload may wait for the printer to take more data before it is
 * reported as stalled. A P1S writing a large file to its SD card pauses mid-file
 * for longer than the 15 s used for FTP commands: an 18.4 MB plate stopped at
 * 13.3 MB for over 15 s (2026-11-06), while 4.6 and 11.5 MB files went through
 * at 130-160 KiB/s. The same stall is what a 4.8 MB A1 mini upload hit on the
 * 15 s limit. A printer that has really stopped is still reported, after this.
 */
export const FTP_UPLOAD_STALL_MS = 120_000;

export type UploadStage = "store" | "verify-size";

export interface UploadFailureDetail {
  stage: UploadStage;
  bytesSent: number;
  bytesTotal: number;
  remotePath: string;
  /** Unset when no SIZE has been tried. Null when SIZE failed. A number is the printer's answer. */
  remoteBytes?: number | null;
  readError?: string;
  secrets?: readonly string[];
}

/** Remove an access code from text we show or log. Short strings are left alone so byte counts stay intact. */
export function redactSecrets(text: string, secrets: readonly string[] = []): string {
  let out = text;
  for (const secret of secrets) {
    if (secret.length >= 8) out = out.split(secret).join("<redacted>");
  }
  return out;
}

/**
 * One sentence for a person and for stderr. Byte counts are the data socket's, not proof the
 * printer stored them; `remoteBytes` is the only stored-size figure, and it never authorizes a print.
 */
export function describeUploadFailure(cause: unknown, detail: UploadFailureDetail): string {
  const secrets = detail.secrets ?? [];
  const remote = detail.remotePath.replace(/^\/+/, "").replace(/[\r\n\0]/g, "");
  const readError = detail.readError === undefined ? undefined : redactSecrets(detail.readError, secrets);
  const stored = detail.remoteBytes === undefined
    ? "the printer's stored size was not read"
    : detail.remoteBytes === null
      ? `the printer's stored size could not be read${readError ? ` (${readError})` : ""}`
      : `the printer reports ${detail.remoteBytes} stored bytes`;
  const causeText = redactSecrets(cause instanceof Error ? cause.message : cause == null ? "" : String(cause), secrets);
  const body = `FTPS upload failed during ${detail.stage}: ${detail.bytesSent} of ${detail.bytesTotal} bytes were handed to the data socket for ${remote}; ${stored}. That file is incomplete or unverified and must not be printed.`;
  return causeText && !body.includes(causeText) ? `${body} ${causeText}` : body;
}

/** A failed or unverified FTPS upload. Callers must not send a print command. */
export class UploadTransferError extends Error {
  readonly stage: UploadStage;
  readonly bytesSent: number;
  readonly bytesTotal: number;
  readonly remotePath: string;
  readonly remoteBytes: number | null | undefined;
  readonly readError: string | undefined;
  readonly reason: unknown;
  readonly logLine: string;

  constructor(cause: unknown, detail: UploadFailureDetail) {
    const message = describeUploadFailure(cause, detail);
    super(message);
    this.name = "UploadTransferError";
    this.reason = cause;
    this.stage = detail.stage;
    this.bytesSent = detail.bytesSent;
    this.bytesTotal = detail.bytesTotal;
    this.remotePath = detail.remotePath;
    this.remoteBytes = detail.remoteBytes;
    this.readError = detail.readError === undefined ? undefined : redactSecrets(detail.readError, detail.secrets);
    this.logLine = `ftps upload: ${message}`;
  }
}

function bytesSentOf(error: unknown): number | undefined {
  if (!error || typeof error !== "object" || !("bytesSent" in error)) return undefined;
  const value = Number((error as { bytesSent: unknown }).bytesSent);
  return Number.isFinite(value) && value >= 0 ? Math.floor(value) : undefined;
}

/**
 * Store bytes on the printer in small writes, allowing the printer's SD card to pause.
 *
 * basic-ftp's TransferWatchdog reports a stall ("Timeout (data socket)") when
 * the data socket's bytesWritten stops changing while data is still queued
 * (`writableLength > 0`). Readable.from(buffer) hands the whole file over as one
 * write, so a pause longer than the client timeout aborts the transfer after STOR
 * has already created the remote name. Small writes keep the count moving as the
 * printer takes each one. The watchdog's limit is the client's timeout, read when
 * the transfer starts; it is raised to stallMs for this transfer (and the wait for
 * the printer's 226 after it) and restored afterwards, so other commands keep theirs.
 * The bytes are the ones already read and inspected; nothing is re-read from disk.
 */
export async function uploadBytes(client: FTPClient, bytes: Buffer, remotePath: string,
  { chunkBytes = FTP_UPLOAD_CHUNK_BYTES, stallMs = FTP_UPLOAD_STALL_MS, onProgress }:
  { chunkBytes?: number; stallMs?: number; onProgress?: (sent: number, total: number) => void } = {}): Promise<void> {
  // Typed readonly, but a plain property that basic-ftp 6.x reads when each transfer starts
  // (TransferResolver.onDataStart/onDataDone); tests/ftp-upload-chunks.test.mjs fails if that changes.
  const context = client.ftp as { timeout: number };
  const commandTimeout = context.timeout;
  context.timeout = Math.max(commandTimeout, stallMs);
  let sent = 0;
  // Bytes the data connection has sent so far, reported to the caller (MCP progress, counterpart/1).
  onProgress?.(0, bytes.length);
  client.trackProgress(info => {
    if (info.type !== "upload") return;
    sent = Math.min(Math.max(0, info.bytes), bytes.length);
    onProgress?.(sent, bytes.length);
  });
  try {
    await client.uploadFrom(Readable.from(chunks(bytes, chunkBytes)), remotePath);
    sent = bytes.length;
    onProgress?.(bytes.length, bytes.length);
  } catch (error) {
    if (error && typeof error === "object") Object.assign(error, { bytesSent: sent });
    throw error;
  } finally {
    context.timeout = commandTimeout;
    client.trackProgress();
  }
}

/**
 * Upload the inspected bytes, then require the printer's SIZE to equal that buffer.
 * A 226 without a matching size, or a size that cannot be read, is a failed upload.
 */
export async function storeVerifiedBytes(client: FTPClient, bytes: Buffer, remotePath: string,
  options?: { chunkBytes?: number; stallMs?: number; onProgress?: (sent: number, total: number) => void }): Promise<void> {
  try {
    await uploadBytes(client, bytes, remotePath, options);
  } catch (error) {
    throw new UploadTransferError(error, {
      stage: "store",
      bytesSent: bytesSentOf(error) ?? 0,
      bytesTotal: bytes.length,
      remotePath,
    });
  }
  let remoteBytes: number | null = null;
  let readError: string | undefined;
  try {
    const size = await client.size(remotePath);
    if (Number.isSafeInteger(size) && size >= 0) remoteBytes = size;
    else readError = "SIZE did not return a file length";
  } catch (error) {
    readError = error instanceof Error ? error.message : String(error);
  }
  if (remoteBytes !== bytes.length) {
    throw new UploadTransferError(readError ? new Error(readError) : undefined, {
      stage: "verify-size",
      bytesSent: bytes.length,
      bytesTotal: bytes.length,
      remotePath,
      remoteBytes,
      readError,
    });
  }
}

/**
 * Read-only SIZE on a connection the caller opens. Failures are returned, not thrown:
 * the upload error stays the reason the print does not start, and this text is attached to it.
 */
export async function measureRemoteFile(
  connect: () => Promise<{ size(path: string): Promise<number>; close(): void }>,
  remotePath: string,
  secrets: readonly string[] = [],
): Promise<{ bytes: number | null; error?: string }> {
  let client: { size(path: string): Promise<number>; close(): void } | undefined;
  try {
    client = await connect();
    const size = await client.size(remotePath);
    if (!Number.isSafeInteger(size) || size < 0) return { bytes: null, error: "SIZE did not return a file length" };
    return { bytes: size };
  } catch (error) {
    return { bytes: null, error: redactSecrets(error instanceof Error ? error.message : String(error), secrets) };
  } finally {
    try { client?.close(); } catch { /* a dead probe must not replace the upload error */ }
  }
}

/**
 * Attach a stored-size reading to a failed upload. Matching SIZE does not turn the failure into success:
 * the transfer already did not finish, so the name must not be printed.
 */
export async function incompleteUpload(
  error: UploadTransferError,
  probe: () => Promise<{ bytes: number | null; error?: string }>,
  secrets: readonly string[] = [],
): Promise<UploadTransferError> {
  let remoteBytes = error.remoteBytes;
  let readError = error.readError;
  if (remoteBytes === undefined) {
    const measured = await probe();
    remoteBytes = measured.bytes;
    readError = measured.error;
  }
  return new UploadTransferError(error.reason, {
    stage: error.stage,
    bytesSent: error.bytesSent,
    bytesTotal: error.bytesTotal,
    remotePath: error.remotePath,
    remoteBytes,
    readError,
    secrets,
  });
}
