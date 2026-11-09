// Load the built playground engine in Node through the same loader the browser
// uses, and assert real command results (no server, no network).
import assert from 'node:assert/strict';
import { readdir, readFile } from 'node:fs/promises';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';
import { Playground } from '../src/lib/playground/ffkv.ts';
import { playgroundRefuses } from './playground-commands.mjs';

const wasm = await readFile(new URL('../public/playground/ffkv.wasm', import.meta.url));
const started = performance.now();
const playground = await Playground.load(wasm);
const loadMs = performance.now() - started;
const a = playground.open();
const b = playground.open();
let checks = 0;

function expect(session, line, output, { error = false, blocked = false } = {}) {
  const reply = session.eval(line);
  assert.equal(reply.output, output, `${line}: output`);
  assert.equal(reply.error, error, `${line}: error flag`);
  assert.equal(reply.blocked, blocked, `${line}: blocked flag`);
  checks += 1;
  return reply;
}
function expectPoll(session, output, label) {
  const reply = session.poll();
  assert.equal(reply.output, output, `${label}: polled output`);
  assert.equal(reply.blocked, false, `${label}: still blocked`);
  checks += 1;
}

assert.ok(a.id >= 1 && b.id >= 1 && a.id !== b.id, 'sessions have distinct ids');
expect(a, 'PING', 'PONG\n');
expect(a, 'SET greeting "hello ffkv"', 'OK\n');
expect(b, 'GET greeting', '"hello ffkv"\n');
expect(a, 'INCR visits', '(integer) 1\n');
expect(a, 'INCRBY visits 9', '(integer) 10\n');
expect(a, 'SET session:42 ada EX 30', 'OK\n');
expect(a, 'TTL session:42', '(integer) 30\n');
expect(a, 'RPUSH letters a b c', '(integer) 3\n');
expect(a, 'LRANGE letters 0 -1', '1) "a"\n2) "b"\n3) "c"\n');
expect(a, 'HSET user:1 name Ada lang Rust', '(integer) 2\n');
expect(b, 'HGET user:1 lang', '"Rust"\n');
expect(a, 'ZADD board 120 ada 95 linus 140 grace', '(integer) 3\n');
expect(a, 'ZREVRANGE board 0 1 WITHSCORES', '1) "grace"\n2) "140"\n3) "ada"\n4) "120"\n');
expect(a, `JSON.SET doc $ '{"langs":["Rust"]}'`, 'OK\n');
expect(a, `JSON.ARRAPPEND doc $.langs '"Zig"'`, '1) (integer) 2\n');
expect(a, 'JSON.GET doc $.langs', '"[[\\"Rust\\",\\"Zig\\"]]"\n');
expect(a, 'GET', "(error) ERR wrong number of arguments for 'get' command\n", { error: true });
expect(a, "SET x 'open", 'ffkv-cli: input: unterminated quoted argument\n', { error: true });

// Each session has its own selected database.
const selected = expect(a, 'SELECT 2', 'OK\n');
assert.equal(selected.prompt, 'ffkv[2]> ');
assert.equal(b.eval('PING').prompt, 'ffkv> ');
expect(a, 'GET greeting', '(nil)\n');
expect(a, 'SELECT 0', 'OK\n');

// A blocking pop in one session is served by a push from the other.
expect(b, 'BLPOP jobs 0', '', { blocked: true });
assert.deepEqual(b.poll(), { output: '', blocked: true, prompt: 'ffkv> ', error: false }, 'BLPOP stays blocked until data arrives');
expect(a, 'LPUSH jobs email.welcome', '(integer) 1\n');
expectPoll(b, '1) "jobs"\n2) "email.welcome"\n', 'BLPOP served');
expect(a, 'LLEN jobs', '(integer) 0\n');

// A blocking pop with a timeout answers nil once the timeout passes.
expect(b, 'BLPOP nothing 0.1', '', { blocked: true });
await sleep(250);
expectPoll(b, '(nil)\n', 'BLPOP timeout');

// Streams: XREAD BLOCK is served by XADD from the other session.
expect(b, 'XREAD BLOCK 0 STREAMS orders $', '', { blocked: true });
const added = a.eval('XADD orders 1-1 item book');
assert.equal(added.output, '"1-1"\n');
expectPoll(b, '1) 1) "orders"\n   2) 1) 1) "1-1"\n         2) 1) "item"\n            2) "book"\n', 'XREAD served');

// Pub/Sub: messages reach the subscriber through poll.
expect(b, 'SUBSCRIBE news', '1) "subscribe"\n2) "news"\n3) (integer) 1\n');
expect(a, 'PUBLISH news hello', '(integer) 1\n');
expectPoll(b, '1) "message"\n2) "news"\n3) "hello"\n', 'Pub/Sub message');
expect(b, 'UNSUBSCRIBE news', '1) "unsubscribe"\n2) "news"\n3) (integer) 0\n');

// RESP3 is per session.
const hello = a.eval('HELLO 3');
assert.equal(hello.error, false);
assert.match(hello.output, /"proto"\n {3}value:\n {3}\(integer\) 3\n/);
// Single-field hash: HGETALL field order is not part of the contract.
expect(a, 'HSET user:9 name Ada', '(integer) 1\n');
expect(a, 'HGETALL user:9', '1) key:\n   "name"\n   value:\n   "Ada"\n');
expect(b, 'HGETALL user:9', '1) "name"\n2) "Ada"\n');

// Out-of-scope subsystems answer a clear error, never a fake success.
const lua = a.eval(`EVAL "return 1" 0`);
assert.equal(lua.error, true);
assert.match(lua.output, /ERR EVAL is not available in the browser playground/);
checks += 1;

// Reference pages link here unless the playground refuses the command. Check that
// claim against the engine for every catalog command, on a fresh connection. Some
// subcommands check arity first, so add placeholder arguments until arity passes.
const refuses = await playgroundRefuses(fileURLToPath(new URL('../../../', import.meta.url)));
const catalog = JSON.parse(await readFile(new URL('../../../src/command_catalog.json', import.meta.url), 'utf8'));
const mismatches = [];
for (const { name } of catalog.commands) {
  let reply;
  for (const extra of ['', ' x', ' x x']) {
    const probe = playground.open();
    reply = probe.eval(name + extra);
    probe.close();
    if (!reply.output.includes('wrong number of arguments')) break;
  }
  if (/is not available in the browser playground/.test(reply.output) !== refuses(name)) mismatches.push(`${name}: ${reply.output.trim()}`);
}
assert.deepEqual(mismatches, [], 'src/wasm/session.rs lists must match what the engine refuses');
checks += catalog.commands.length;

// Every generated "Try in playground" link must work: replay its commands in order on a
// fresh dataset and connection; none may error or block, and the example must not be
// left queued in an open MULTI. Links that only pre-fill a command name are counted.
const commandPages = new URL('../src/content/docs/reference/commands/', import.meta.url);
const broken = [];
let sequences = 0;
let fallbacks = 0;
for (const file of (await readdir(commandPages)).filter((file) => file.endsWith('.md')).sort()) {
  const link = (await readFile(new URL(file, commandPages), 'utf8')).match(/\[Try in playground\]\(\/playground\/\?([^)]*)\)/);
  if (!link) continue;
  const lines = new URLSearchParams(link[1]).getAll('cmd');
  assert.ok(lines.length, `${file}: playground link without commands`);
  if (lines.length === 1 && lines[0].endsWith(' ')) {
    fallbacks += 1;
    continue;
  }
  sequences += 1;
  playground.reset();
  const session = playground.open();
  for (const [index, line] of lines.entries()) {
    const reply = session.eval(line);
    if (reply.error || reply.blocked || (index === lines.length - 1 && reply.output === 'QUEUED\n')) {
      broken.push(`${file} step ${index + 1}/${lines.length} ${line}: ${reply.blocked ? 'blocked' : reply.output.trim()}`);
      break;
    }
  }
  session.close();
}
assert.deepEqual(broken, [], 'every generated playground sequence must replay cleanly');
assert.ok(sequences > 0, 'expected generated playground sequences; run npm run generate first');
checks += sequences;

// Reset empties every database; sessions stay usable.
playground.reset();
expect(a, 'DBSIZE', '(integer) 0\n');
b.close();
assert.equal(playground.sessions.size, 1);
assert.throws(() => b.eval('PING'), /closed/);
a.close();

console.log(`Playground smoke passed: ${checks} checks against the real engine, including ${sequences} reference-page sequences replayed (${fallbacks} pages pre-fill only the command name). Load ${loadMs.toFixed(0)} ms, ${(wasm.length / 1024 / 1024).toFixed(2)} MiB.`);
