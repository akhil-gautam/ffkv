export type Json = null | boolean | number | string | Json[] | { [key: string]: Json };
export type Bytes = { base64: string; utf8: string | null; bytes: number; redacted: boolean };
export type Resp =
  | { type: 'simple'; value: string }
  | { type: 'error'; message: string }
  | { type: 'integer' | 'double'; value: number | string }
  | { type: 'boolean'; value: boolean }
  | { type: 'bulk'; value: Bytes }
  | { type: 'null' }
  | { type: 'array' | 'set'; value: Resp[] }
  | { type: 'map'; value: [Resp, Resp][] }
  | { type: 'scored'; value: { member: Bytes; score: number | string }[] };
export type Session = { name: string; role: 'viewer' | 'admin'; can_reveal: boolean };
export type Metrics = {
  observed_at_ms: number; uptime_seconds: number; commands_total: number; errors_total: number;
  connections: number; blocked_clients: number; rss_bytes: number | null;
  latency: { samples: number; p50_us: number | null; p95_us: number | null; p99_us: number | null };
  wal: { bytes: number; policy: string; dirty: boolean; last_sync_ms: number | null };
  replication: { role: string; status: string; lag: number | null };
};
export type Overview = {
  server: { name: string; version: string; redis_baseline: string; shards: number; storage_mode: string; durability: string; role: string };
  metrics: Metrics; storage: Json; key_count: number | string;
};
export type KeyRow = { key: Bytes; type: string; ttl_ms: number | string };
export type KeyDetail = KeyRow & { placement: string; result: Resp; cursor: string | null };
export type Command = { name: string; arity: number; flags: string[]; readonly: boolean; syntax: string; baseline: string; limits: string[] };
export type Catalog = { commands: Command[]; clients: { name: string; version: string }[] };
export type CommandResult = { result: Resp; elapsed_us: number };
export type QueueRow = { key: Bytes; kind: 'native' | 'sidekiq'; stats: Resp };
export type QueueDetail = { key: Bytes; kind?: 'native' | 'sidekiq'; stats?: Resp; result?: Resp; jobs?: Resp; cursor: string | null; [key: string]: unknown };
export type AuditPage = { cursor: string | null; rows?: Json[]; records?: Json[]; entries?: Json[] };
export type Storage = { mode: string; stats: Json; wal: Resp; limits: Json };

export class ApiError extends Error {
  constructor(public status: number, message: string) { super(message); }
}
export async function request<T>(token: string, path: string, body?: unknown, signal?: AbortSignal): Promise<T> {
  const res = await fetch(path, {
    method: body === undefined ? 'GET' : 'POST',
    headers: { Authorization: `Bearer ${token}`, ...(body === undefined ? {} : { 'Content-Type': 'application/json' }) },
    ...(body === undefined ? {} : { body: JSON.stringify(body) }),
    signal, credentials: 'omit', cache: 'no-store',
  });
  if (!res.ok) {
    const text = await res.text();
    let message = `${res.status} ${res.statusText}`;
    try { const error = JSON.parse(text); message = typeof error.error === 'string' ? error.error : typeof error.message === 'string' ? error.message : message; } catch { /* Never reflect an arbitrary HTML error document. */ }
    throw new ApiError(res.status, message);
  }
  return res.json() as Promise<T>;
}
export function query(path: string, params: Record<string, string | number | boolean>): string {
  return `${path}?${new URLSearchParams(Object.entries(params).map(([k, v]) => [k, String(v)]))}`;
}
export function encode(text: string): string {
  const bytes = new TextEncoder().encode(text);
  let binary = '';
  for (let i = 0; i < bytes.length; i++) binary += String.fromCharCode(bytes[i]);
  return btoa(binary);
}
export function validBase64(value: string): boolean {
  try { return btoa(atob(value)) === value; } catch { return false; }
}
export function byteLabel(value: Bytes): string {
  const hidden = value.redacted && value.utf8 === null && value.base64 === '';
  return hidden ? `[redacted · ${value.bytes} bytes]` : value.utf8 ?? `base64:${value.base64}`;
}
export function ttl(value: number | string): string {
  if (String(value) === '-1') return 'No expiry';
  if (String(value) === '-2') return 'Missing';
  return `${value} ms`;
}
export function pretty(value: unknown): string { return JSON.stringify(value, null, 2) ?? String(value); }
export function number(value: number | string | null | undefined): string {
  return value == null ? 'Unavailable' : typeof value === 'number' ? new Intl.NumberFormat().format(value) : value;
}
export function size(value: number | null | undefined): string {
  if (value == null) return 'Unavailable';
  if (value < 1024) return `${value} B`;
  const unit = Math.min(Math.floor(Math.log(value) / Math.log(1024)), 4);
  return `${(value / 1024 ** unit).toFixed(1)} ${['B', 'KiB', 'MiB', 'GiB', 'TiB'][unit]}`;
}

// Deliberately a tokenizer, not a shell: no expansion, substitution or execution.
export function tokenize(input: string): string[] {
  const args: string[] = []; let word = ''; let quote = ''; let escaped = false; let started = false;
  for (const char of input) {
    if (escaped) { word += char === 'n' ? '\n' : char === 'r' ? '\r' : char === 't' ? '\t' : char; escaped = false; started = true; }
    else if (char === '\\' && quote !== "'") { escaped = true; started = true; }
    else if (quote) { if (char === quote) quote = ''; else word += char; }
    else if (char === '"' || char === "'") { quote = char; started = true; }
    else if (/\s/.test(char)) { if (started) { args.push(word); word = ''; started = false; } }
    else { word += char; started = true; }
  }
  if (escaped || quote) throw new Error('Close the quote or trailing escape before running.');
  if (started) args.push(word);
  if (!args.length) throw new Error('Enter a command vector.');
  return args;
}
