import { useMemo, useState } from 'react';
import { useQueryClient } from '@tanstack/react-query';
import { encode, number, pretty, tokenize, validBase64, type CommandResult } from './api';
import { ErrorNotice, Loading, Notice, PageTitle, Panel, RespView } from './components';
import { useCatalog, useRuntime } from './runtime';

export function CommandRunner({ initial = '', initialMode = 'text', title = 'Command vector' }: { initial?: string; initialMode?: 'text' | 'base64'; title?: string }) {
  const { session, post, confirm } = useRuntime(); const catalog = useCatalog(); const client = useQueryClient();
  const [input, setInput] = useState(initial); const [mode, setMode] = useState(initialMode);
  const [writes, setWrites] = useState(false); const [busy, setBusy] = useState(false);
  const [result, setResult] = useState<CommandResult | null>(null); const [error, setError] = useState<unknown>(null);
  const vector = useMemo(() => {
    try {
      if (mode === 'text') { const args = tokenize(input); return { args: args.map(encode), name: args[0].toUpperCase(), error: '' }; }
      const parsed: unknown = JSON.parse(input);
      if (!Array.isArray(parsed) || !parsed.length || !parsed.every(value => typeof value === 'string' && validBase64(value))) throw new Error('Enter a JSON array of canonical base64 strings, including the command name.');
      return { args: parsed as string[], name: atob(parsed[0]).toUpperCase(), error: '' };
    } catch (e) { return { args: [], name: '', error: e instanceof Error ? e.message : 'Invalid vector' }; }
  }, [input, mode]);
  const descriptor = catalog.data?.commands.find(c => c.name.toUpperCase() === vector.name);
  const canWrite = session.role === 'admin';
  const execute = async (reveal: boolean, confirmed: boolean) => {
    setBusy(true); setError(null); setResult(null);
    try {
      const response = await post<CommandResult>('/api/command', { args_b64: vector.args, reveal, confirmed });
      setResult(response);
      if (!descriptor?.readonly) await client.invalidateQueries({ predicate: q => q.queryKey[0] !== 'commands' });
    } catch (e) { setError(e); throw e; }
    finally { setBusy(false); }
  };
  const run = (reveal = false) => {
    setError(null);
    if (vector.error) { setError(new Error(vector.error)); return; }
    if (!descriptor) { setError(new Error('This command is not in the running engine catalog.')); return; }
    if (!descriptor.readonly && (!canWrite || !writes)) { setError(new Error('Write commands are disabled. An administrator must enable write mode first.')); return; }
    if (reveal && (!canWrite || !session.can_reveal)) return;
    if (!descriptor.readonly || reveal) {
      confirm({ title: reveal ? 'Run command and reveal payloads?' : `Execute ${descriptor.name}?`, description: `${descriptor.readonly ? 'This reads server data.' : 'This changes live server state.'} ${reveal ? 'Payloads will become visible on this screen; protected JSON fields remain masked by server policy.' : 'Response payloads remain redacted.'} Check your argument vector before approving.`, confirmLabel: reveal ? 'Run and reveal' : 'Execute write', run: () => execute(reveal, true) });
    } else void execute(false, false).catch(() => {});
  };
  return <Panel title={title} note="Separate management authorization · never a system shell">
    <div className="editor-toolbar"><label>Input format<select value={mode} onChange={e => { setMode(e.target.value as 'text' | 'base64'); setInput(''); }}><option value="text">Quoted text vector</option><option value="base64">Binary-safe base64 JSON vector</option></select></label><label className="check"><input type="checkbox" checked={writes} disabled={!canWrite} onChange={e => setWrites(e.target.checked)} />Enable writes {canWrite ? '' : '(admin only)'}</label></div>
    <label className="field editor-field"><span>{mode === 'text' ? 'Command and arguments' : 'Base64 argument array'}</span><textarea value={input} spellCheck={false} onChange={e => setInput(e.target.value)} onKeyDown={e => { if ((e.ctrlKey || e.metaKey) && e.key === 'Enter') { e.preventDefault(); run(); } }} aria-describedby="tokenizer-help" placeholder={mode === 'text' ? 'Enter a command from the engine catalog' : '["R0VU", "a2V5"]'} /></label>
    <p id="tokenizer-help" className="help">{mode === 'text' ? 'Whitespace separates arguments; single/double quotes preserve spaces. Backslash escapes n, r, t and literal characters. No variables, pipes, globbing or shell execution.' : 'Each JSON string represents one exact argument as base64. Key names and binary values round-trip without UTF-8 conversion.'} <kbd>⌘ / Ctrl ↵</kbd> to run.</p>
    {mode === 'text' && !/\s/.test(input) && input.length > 0 && <div className="suggestions" aria-label="Command suggestions">{catalog.data?.commands.filter(c => c.name.toLowerCase().startsWith(input.toLowerCase())).slice(0, 12).map(c => <button key={c.name} onClick={() => setInput(`${c.name} `)}>{c.name}<span>{c.readonly ? 'read' : 'write'}</span></button>)}</div>}
    {catalog.isPending && <Loading />}{catalog.isError && <ErrorNotice error={catalog.error} retry={() => void catalog.refetch()} />}
    {descriptor && <div className="command-help"><code>{descriptor.syntax || descriptor.name}</code><span className={`badge ${descriptor.readonly ? '' : 'warning'}`}>{descriptor.readonly ? 'Read only' : 'Writes state'}</span><p>Arity {descriptor.arity} · {descriptor.flags.join(', ') || 'No flags reported'} · {descriptor.baseline || 'Baseline not reported'}</p>{descriptor.limits.length > 0 && <ul>{descriptor.limits.map(limit => <li key={limit}>{limit}</li>)}</ul>}</div>}
    <div className="editor-actions"><button className="primary" disabled={busy || !input.trim() || !catalog.data} onClick={() => run()}>{busy ? 'Running…' : 'Run redacted'}</button><button disabled={busy || !input.trim() || !session.can_reveal || !canWrite || !catalog.data} onClick={() => run(true)}>Run with payload reveal</button><span className="muted">{writes ? 'Write mode enabled · confirmation required' : 'Read-only by default'}</span></div>
    {error != null && <ErrorNotice error={error} />}
    {result && <section className="command-result" aria-label="Command result"><div className="result-heading"><strong>Response</strong><span>{number(result.elapsed_us)} µs · server execution</span><button onClick={() => setResult(null)}>Clear result</button></div><RespView value={result.result} /></section>}
  </Panel>;
}
export function Commands() {
  return <><PageTitle eyebrow="Operator tools" title="Command console" description="Inspect or operate through the actual engine command catalog. All write and reveal operations require explicit approval." /><Notice>Payloads are redacted by default, including bulk replies. Unsupported and connection-state commands are rejected by the management service.</Notice><CommandRunner /></>;
}
export function keyEditVector(keyBase64: string, type: string) {
  const command = type === 'string' ? 'SET' : type === 'list' ? 'RPUSH' : type === 'set' ? 'SADD' : type === 'hash' ? 'HSET' : type === 'zset' ? 'ZADD' : 'DEL';
  return pretty([encode(command), keyBase64]);
}
