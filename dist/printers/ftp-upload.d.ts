import type { Client as FTPClient } from "basic-ftp";
/** Bytes per write on the FTPS data socket; matches fs.createReadStream's default. */
export declare const FTP_UPLOAD_CHUNK_BYTES: number;
/**
 * How long a print upload may wait for the printer to take more data before it is
 * reported as stalled. A P1S writing a large file to its SD card pauses mid-file
 * for longer than the 15 s used for FTP commands: an 18.4 MB plate stopped at
 * 13.3 MB for over 15 s (2026-11-06), while 4.6 and 11.5 MB files went through
 * at 130-160 KiB/s. The same stall is what a 4.8 MB A1 mini upload hit on the
 * 15 s limit. A printer that has really stopped is still reported, after this.
 */
export declare const FTP_UPLOAD_STALL_MS = 120000;
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
export declare function redactSecrets(text: string, secrets?: readonly string[]): string;
/**
 * One sentence for a person and for stderr. Byte counts are the data socket's, not proof the
 * printer stored them; `remoteBytes` is the only stored-size figure, and it never authorizes a print.
 */
export declare function describeUploadFailure(cause: unknown, detail: UploadFailureDetail): string;
/** A failed or unverified FTPS upload. Callers must not send a print command. */
export declare class UploadTransferError extends Error {
    readonly stage: UploadStage;
    readonly bytesSent: number;
    readonly bytesTotal: number;
    readonly remotePath: string;
    readonly remoteBytes: number | null | undefined;
    readonly readError: string | undefined;
    readonly reason: unknown;
    readonly logLine: string;
    constructor(cause: unknown, detail: UploadFailureDetail);
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
export declare function uploadBytes(client: FTPClient, bytes: Buffer, remotePath: string, { chunkBytes, stallMs, onProgress }?: {
    chunkBytes?: number;
    stallMs?: number;
    onProgress?: (sent: number, total: number) => void;
}): Promise<void>;
/**
 * Upload the inspected bytes, then require the printer's SIZE to equal that buffer.
 * A 226 without a matching size, or a size that cannot be read, is a failed upload.
 */
export declare function storeVerifiedBytes(client: FTPClient, bytes: Buffer, remotePath: string, options?: {
    chunkBytes?: number;
    stallMs?: number;
    onProgress?: (sent: number, total: number) => void;
}): Promise<void>;
/**
 * Read-only SIZE on a connection the caller opens. Failures are returned, not thrown:
 * the upload error stays the reason the print does not start, and this text is attached to it.
 */
export declare function measureRemoteFile(connect: () => Promise<{
    size(path: string): Promise<number>;
    close(): void;
}>, remotePath: string, secrets?: readonly string[]): Promise<{
    bytes: number | null;
    error?: string;
}>;
/**
 * Attach a stored-size reading to a failed upload. Matching SIZE does not turn the failure into success:
 * the transfer already did not finish, so the name must not be printed.
 */
export declare function incompleteUpload(error: UploadTransferError, probe: () => Promise<{
    bytes: number | null;
    error?: string;
}>, secrets?: readonly string[]): Promise<UploadTransferError>;
