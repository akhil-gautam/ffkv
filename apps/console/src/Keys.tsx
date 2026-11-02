import { useState, type FormEvent } from 'react';
import { useQuery } from '@tanstack/react-query';
import { byteLabel, encode, query, ttl, validBase64, type Bytes, type KeyDetail, type KeyRow } from './api';
import { ByteValue, ErrorNotice, Loading, Notice, PageTitle, Pager, Panel, RespView, VirtualRows } from './components';
import { CommandRunner, keyEditVector } from './Commands';
import { useRuntime } from './runtime';

function KeyInspector({ selected, close }: { selected: Bytes; close: () => void }) {
  const { get, session, confirm } = useRuntime(); const [cursor, setCursor] = useState('0'); const [trail, setTrail] = useState<string[]>([]); const [reveal, setReveal] = useState(false); const [edit, setEdit] = useState(false);
  const detail = useQuery({ queryKey: ['key', selected.base64, cursor, reveal], queryFn: ({ signal }) => get<KeyDetail>(query('/api/key', { key: selected.base64, cursor, count: 100, reveal }), signal) });
  return <section className="inspector" aria-label="Key inspector"><div className="inspector-heading"><div><div className="eyebrow">Key inspector</div><h2><ByteValue value={selected} /></h2></div><button onClick={close}>Close inspector</button></div>
    <div className="key-encoding"><span className="muted">Exact key · base64</span><code>{selected.base64 || '(empty key)'}</code></div>
    {detail.isPending ? <Loading /> : detail.isError ? <ErrorNotice error={detail.error} retry={() => void detail.refetch()} /> : <>
      <div className="detail-summary"><span className="badge">{detail.data.type}</span><span>TTL: {ttl(detail.data.ttl_ms)}</span><span>Placement: {detail.data.placement}</span></div>
      <div className="toolbar"><button onClick={() => void detail.refetch()}>Refresh</button>{reveal ? <button onClick={() => { setReveal(false); }}>Hide payloads</button> : <button disabled={session.role !== 'admin' || !session.can_reveal} onClick={() => confirm({ title: 'Reveal key payloads?', description: `Expose payloads for ${byteLabel(selected)} on this screen. Protected JSON fields remain masked. This read is audited.`, confirmLabel: 'Reveal payloads', run: async () => { setReveal(true); } })}>Reveal payloads</button>}<button disabled={session.role !== 'admin'} onClick={() => setEdit(v => !v)}>{edit ? 'Close command editor' : 'Edit via command vector'}</button></div>
      {reveal && <Notice>Revealed payloads are visible. Hide them before leaving this screen unattended.</Notice>}
      <Panel title="Value" note={reveal ? 'Explicit reveal active' : 'Payloads redacted'}><RespView value={detail.data.result} /><Pager cursor={detail.data.cursor} busy={detail.isFetching} next={() => { setTrail(t => [...t, cursor]); setCursor(detail.data.cursor!); }} back={trail.length ? () => { setCursor(trail.at(-1)!); setTrail(t => t.slice(0, -1)); } : undefined} /></Panel>
      {edit && <><Notice>Complete the exact argument vector below using the command syntax. Each value is base64. The key is already encoded without conversion. Writes stay disabled until you enable them, and each write requires confirmation.</Notice><CommandRunner key={`${selected.base64}:${detail.data.type}`} initial={keyEditVector(selected.base64, detail.data.type)} initialMode="base64" title="Edit key through supported commands" /></>}
    </>}
  </section>;
}
export function Keys() {
  const { get } = useRuntime(); const [match, setMatch] = useState('*'); const [draft, setDraft] = useState('*'); const [cursor, setCursor] = useState('0'); const [trail, setTrail] = useState<string[]>([]); const [selected, setSelected] = useState<Bytes | null>(null);
  const [direct, setDirect] = useState(''); const [directMode, setDirectMode] = useState('utf8'); const [directError, setDirectError] = useState('');
  const keys = useQuery({ queryKey: ['keys', match, cursor], queryFn: ({ signal }) => get<{ cursor: string; keys: KeyRow[] }>(query('/api/keys', { cursor, match, count: 100 }), signal) });
  const search = (e: FormEvent) => { e.preventDefault(); setCursor('0'); setTrail([]); setMatch(draft); };
  const openDirect = (e: FormEvent) => {
    e.preventDefault(); const base64 = directMode === 'base64' ? direct : encode(direct);
    if (!validBase64(base64)) { setDirectError('Use canonical base64, including padding.'); return; }
    setDirectError(''); setSelected({ base64, utf8: directMode === 'utf8' ? direct : null, bytes: atob(base64).length, redacted: false });
  };
  return <><PageTitle eyebrow="Database" title="Key explorer" description="Cursor-based inspection across implemented datatypes. Exact binary key names, real TTLs and honest storage placement." />
    <Panel title="Browse keys" note="SCAN only · bounded server pages"><form className="toolbar search-toolbar" onSubmit={search}><label className="field grow"><span>Key pattern (Redis glob)</span><input value={draft} onChange={e => setDraft(e.target.value)} spellCheck={false} /></label><button type="submit" className="primary">Scan keys</button></form>
      {keys.isPending ? <Loading /> : keys.isError ? <ErrorNotice error={keys.error} retry={() => void keys.refetch()} /> : <><div className="table-heading key-grid"><span>Key name</span><span>Type</span><span>TTL</span></div><VirtualRows label="Keys" rows={keys.data.keys} render={row => <button className={`table-row key-grid ${selected?.base64 === row.key.base64 ? 'selected' : ''}`} onClick={() => setSelected(row.key)}><ByteValue value={row.key} /><span className="badge">{row.type}</span><span className="muted">{ttl(row.ttl_ms)}</span></button>} /><Pager cursor={keys.data.cursor} busy={keys.isFetching} next={() => { setTrail(t => [...t, cursor]); setCursor(keys.data.cursor); }} back={trail.length ? () => { setCursor(trail.at(-1)!); setTrail(t => t.slice(0, -1)); } : undefined} /><p className="help">SCAN may return an empty page before completion or repeat keys while the database changes. Continue until the cursor completes.</p></>}
    </Panel>
    <Panel title="Open an exact key" note="Binary-safe direct lookup"><form className="toolbar" onSubmit={openDirect}><label className="field"><span>Encoding</span><select value={directMode} onChange={e => setDirectMode(e.target.value)}><option value="utf8">UTF-8</option><option value="base64">Base64</option></select></label><label className="field grow"><span>Exact key (empty is valid)</span><input value={direct} onChange={e => setDirect(e.target.value)} spellCheck={false} /></label><button type="submit">Inspect key</button></form>{directError && <Notice error>{directError}</Notice>}</Panel>
    {selected && <KeyInspector key={selected.base64} selected={selected} close={() => setSelected(null)} />}
  </>;
}
