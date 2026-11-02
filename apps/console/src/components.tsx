import { useEffect, useRef, useState, type ReactNode } from 'react';
import { useVirtualizer } from '@tanstack/react-virtual';
import { byteLabel, pretty, type Bytes, type Resp } from './api';

export function Notice({ children, error = false }: { children: ReactNode; error?: boolean }) {
  return <div className={`notice ${error ? 'error' : ''}`} role={error ? 'alert' : 'status'}>{children}</div>;
}
export function ErrorNotice({ error, retry }: { error: unknown; retry?: () => void }) {
  return <Notice error>{error instanceof Error ? error.message : 'The request could not be completed.'}{retry && <button className="text-button" onClick={retry}>Try again</button>}</Notice>;
}
export function Loading() { return <Notice>Fetching live server data…</Notice>; }
export function PageTitle({ eyebrow, title, description, actions }: { eyebrow: string; title: string; description: string; actions?: ReactNode }) {
  return <header className="page-title"><div><div className="eyebrow">{eyebrow}</div><h1>{title}</h1><p>{description}</p></div>{actions && <div className="actions">{actions}</div>}</header>;
}
export function Panel({ title, note, children, className = '' }: { title: string; note?: string; children: ReactNode; className?: string }) {
  return <section className={`panel ${className}`}><div className="panel-heading"><h2>{title}</h2>{note && <span>{note}</span>}</div>{children}</section>;
}
export function ByteValue({ value }: { value: Bytes }) {
  const protection = value.utf8 === null && value.base64 === '' ? 'Payload is redacted' : 'Sanitized payload; protected JSON fields are masked';
  return <span className={`byte-value ${value.redacted ? 'muted' : ''}`} title={value.redacted ? protection : `${value.bytes} bytes · base64: ${value.base64}`}>{byteLabel(value)}</span>;
}
export function VirtualRows<T>({ rows, render, label, height = 420 }: { rows: T[]; render: (row: T, index: number) => ReactNode; label: string; height?: number }) {
  const ref = useRef<HTMLDivElement>(null);
  const virtual = useVirtualizer({ count: rows.length, getScrollElement: () => ref.current, estimateSize: () => 64, overscan: 6 });
  if (!rows.length) return <div className="empty">No rows in this page.</div>;
  return <div className="virtual-scroll" ref={ref} style={{ height: Math.min(height, rows.length * 70 + 4) }} role="list" aria-label={label} tabIndex={0}>
    <div style={{ height: virtual.getTotalSize(), width: '100%', position: 'relative' }}>
      {virtual.getVirtualItems().map(item => <div role="listitem" key={item.key} data-index={item.index} ref={virtual.measureElement} className="virtual-row" style={{ position: 'absolute', top: 0, left: 0, width: '100%', transform: `translateY(${item.start}px)` }}>{render(rows[item.index], item.index)}</div>)}
    </div>
  </div>;
}
export function RespView({ value }: { value: Resp }) {
  if (value.type === 'null') return <div className="empty">Null · no value returned.</div>;
  if (value.type === 'error') return <Notice error>{value.message}</Notice>;
  if (value.type === 'bulk') return <div className="payload"><ByteValue value={value.value} /></div>;
  if (value.type === 'array' || value.type === 'set') return <VirtualRows label={`${value.type} response`} rows={value.value} render={(row, i) => <div className="response-row"><span className="row-index">{i}</span><div><RespView value={row} /></div></div>} />;
  if (value.type === 'map') return <VirtualRows label="Map response" rows={value.value} render={([key, val]) => <div className="response-row map-row"><RespView value={key} /><RespView value={val} /></div>} />;
  if (value.type === 'scored') return <VirtualRows label="Sorted set response" rows={value.value} render={row => <div className="data-row"><ByteValue value={row.member} /><code>{row.score}</code></div>} />;
  return <pre className="payload">{String(value.value)}</pre>;
}
export function JsonView({ value }: { value: unknown }) {
  if (value == null) return <div className="empty">Not available from this server.</div>;
  if (Array.isArray(value)) return <VirtualRows label="Structured records" rows={value} render={row => <pre className="payload">{pretty(row)}</pre>} />;
  if (typeof value === 'object') return <dl className="metadata">{Object.entries(value).map(([key, val]) => <div key={key}><dt>{key.replaceAll('_', ' ')}</dt><dd>{val != null && typeof val === 'object' ? <JsonView value={val} /> : val == null ? <span className="muted">Unavailable</span> : String(val)}</dd></div>)}</dl>;
  return <pre className="payload">{String(value)}</pre>;
}
export function Pager({ cursor, next, back, busy }: { cursor: string | null; next: () => void; back?: () => void; busy?: boolean }) {
  return <div className="pager"><span className="muted">Bounded server page · up to 100 rows</span><div>{back && <button disabled={busy} onClick={back}>Previous page</button>}<button disabled={busy || !cursor || cursor === '0'} onClick={next}>Next page <span aria-hidden="true">→</span></button></div></div>;
}
export type Confirmation = { title: string; description: string; confirmLabel?: string; run: () => Promise<void> };
export function ConfirmDialog({ action, close }: { action: Confirmation | null; close: () => void }) {
  const dialog = useRef<HTMLDialogElement>(null);
  const [pending, setPending] = useState(false);
  const running = useRef(false);
  const [error, setError] = useState('');
  useEffect(() => {
    if (action) { dialog.current?.showModal(); setError(''); }
    else dialog.current?.close();
  }, [action]);
  return <dialog ref={dialog} aria-labelledby="confirm-title" onCancel={e => { if (running.current) e.preventDefault(); else close(); }}>
    <div className="dialog-content"><div className="eyebrow">Explicit administrator approval</div><h2 id="confirm-title">{action?.title}</h2><p>{action?.description}</p><p className="muted">This operation is recorded in the server audit log.</p><p role="alert" className="error-text">{error}</p>
      <div className="dialog-actions"><button autoFocus onClick={close} disabled={pending}>Cancel</button><button className="danger" disabled={pending} onClick={async () => {
        if (!action || running.current) return;
        running.current = true; setPending(true); setError('');
        try { await action.run(); close(); } catch (e) { setError(e instanceof Error ? e.message : 'Operation failed.'); }
        finally { running.current = false; setPending(false); }
      }}>{pending ? 'Working…' : action?.confirmLabel || 'Confirm operation'}</button></div>
    </div>
  </dialog>;
}
