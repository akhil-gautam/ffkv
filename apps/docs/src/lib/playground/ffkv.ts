// Loader for the ffkv browser playground: the server's real command engine
// compiled to wasm32-wasip1 (crates/ffkv-wasm). Plain ESM with erasable types
// only, so the same file runs in the browser and in Node (scripts/playground-smoke.mjs).
import { ConsoleStdout, File, OpenFile, WASI } from '@bjorn3/browser_wasi_shim';

/** One reply from `ffkv_eval` / `ffkv_poll`. `output` is exactly what ffkv-cli would print. */
export interface Reply {
  output: string;
  blocked: boolean;
  prompt: string;
  error: boolean;
}

interface Exports {
  memory: WebAssembly.Memory;
  _initialize?: () => unknown;
  ffkv_alloc(len: number): number;
  ffkv_free(ptr: number, len: number): void;
  ffkv_open(): number;
  ffkv_close(id: number): void;
  ffkv_eval(id: number, ptr: number, len: number): bigint;
  ffkv_poll(id: number): bigint;
  ffkv_reset(): void;
}

const REQUIRED = ['memory', 'ffkv_alloc', 'ffkv_free', 'ffkv_open', 'ffkv_close', 'ffkv_eval', 'ffkv_poll', 'ffkv_reset'] as const;
const encoder = new TextEncoder();
const decoder = new TextDecoder('utf-8', { fatal: true });

/** Where to get the module: a URL (fetched) or the module bytes. */
export type WasmSource = string | URL | ArrayBuffer | Uint8Array;

async function compile(source: WasmSource): Promise<WebAssembly.Module> {
  if (source instanceof ArrayBuffer || source instanceof Uint8Array) return WebAssembly.compile(source as BufferSource);
  const response = await fetch(source);
  if (!response.ok) throw new Error(`Could not download ${String(source)}: HTTP ${response.status}`);
  // Streaming compilation needs the application/wasm content type; fall back to bytes otherwise.
  if (response.headers.get('content-type')?.split(';')[0].trim() === 'application/wasm') {
    return WebAssembly.compileStreaming(response);
  }
  return WebAssembly.compile(await response.arrayBuffer());
}

/** Decode the `(ptr << 32) | len` JSON reply returned by `ffkv_eval`/`ffkv_poll`, then free it. */
function takeReply(exports: Exports, packed: bigint): Reply {
  const ptr = Number(packed >> 32n);
  const len = Number(packed & 0xffff_ffffn);
  // Take the view after the call: the module may have grown its memory.
  const json = decoder.decode(new Uint8Array(exports.memory.buffer, ptr, len));
  exports.ffkv_free(ptr, len);
  return JSON.parse(json) as Reply;
}

export class Playground {
  readonly #exports: Exports;
  readonly #sessions = new Set<Session>();

  private constructor(exports: Exports) {
    this.#exports = exports;
  }

  /** Download (or take) the module, instantiate it under WASI and run its reactor initializer. */
  static async load(source: WasmSource): Promise<Playground> {
    const module = await compile(source);
    const log = (line: string) => console.warn(`[ffkv wasm] ${line}`);
    const wasi = new WASI([], [], [new OpenFile(new File([])), ConsoleStdout.lineBuffered(log), ConsoleStdout.lineBuffered(log)]);
    const instance = await WebAssembly.instantiate(module, { wasi_snapshot_preview1: wasi.wasiImport });
    for (const name of REQUIRED) {
      if (!(name in instance.exports)) throw new Error(`The ffkv WebAssembly module does not export ${name}`);
    }
    const exports = instance.exports as unknown as Exports;
    wasi.initialize({ exports });
    return new Playground(exports);
  }

  /** Open a client session. All sessions share one dataset. */
  open(): Session {
    const session: Session = new Session(this.#exports, this.#exports.ffkv_open(), () => this.#sessions.delete(session));
    this.#sessions.add(session);
    return session;
  }

  /** Every session that has not been closed. */
  get sessions(): ReadonlySet<Session> {
    return this.#sessions;
  }

  /** Empty every database. Sessions stay open. */
  reset(): void {
    this.#exports.ffkv_reset();
  }
}

export class Session {
  readonly #exports: Exports;
  readonly #onClose: () => void;
  readonly id: number;
  #closed = false;

  /** Use `Playground.open()`. */
  constructor(exports: Exports, id: number, onClose: () => void) {
    this.#exports = exports;
    this.#onClose = onClose;
    this.id = id;
  }

  /** Run one command line, written exactly as you would type it in ffkv-cli. */
  eval(line: string): Reply {
    this.#check();
    const bytes = encoder.encode(line);
    const len = bytes.length;
    const ptr = len ? this.#exports.ffkv_alloc(len) : 0;
    if (len) new Uint8Array(this.#exports.memory.buffer, ptr, len).set(bytes);
    try {
      return takeReply(this.#exports, this.#exports.ffkv_eval(this.id, ptr, len));
    } finally {
      if (len) this.#exports.ffkv_free(ptr, len);
    }
  }

  /** Output that became ready since the last eval/poll: served blocking commands and Pub/Sub messages. */
  poll(): Reply {
    this.#check();
    return takeReply(this.#exports, this.#exports.ffkv_poll(this.id));
  }

  /** End the session, discarding its blocked command and subscriptions. */
  close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#exports.ffkv_close(this.id);
    this.#onClose();
  }

  get closed(): boolean {
    return this.#closed;
  }

  #check(): void {
    if (this.#closed) throw new Error(`ffkv session ${this.id} is closed`);
  }
}
