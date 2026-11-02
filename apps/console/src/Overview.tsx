import { useQuery } from '@tanstack/react-query';
import { number, size, type Overview as OverviewData, type Metrics } from './api';
import { ErrorNotice, JsonView, Loading, PageTitle, Panel } from './components';
import { useRuntime } from './runtime';

function Metric({ label, value, note }: { label: string; value: string; note: string }) {
  return <div className="metric-card"><span className="metric-label">{label}</span><strong>{value}</strong><span className="muted">{note}</span></div>;
}
function ObservedChart({ history }: { history: Metrics[] }) {
  const points = history.slice(1).flatMap((current, i) => {
    const previous = history[i]; const seconds = (current.observed_at_ms - previous.observed_at_ms) / 1000;
    return seconds > 0 ? [{ time: current.observed_at_ms, rate: (current.commands_total - previous.commands_total) / seconds }] : [];
  });
  if (points.length < 2) return <div className="chart-empty">Collecting observations<span>Two measured intervals are needed. No historical data is invented.</span></div>;
  const start = points[0].time; const end = points.at(-1)!.time; const max = Math.max(...points.map(p => p.rate), 1);
  const path = points.map((point, i) => `${i ? 'L' : 'M'}${45 + (point.time - start) / (end - start) * 900},${185 - point.rate / max * 150}`).join(' ');
  return <div className="chart"><svg viewBox="0 0 1000 230" role="img" aria-label={`Observed command rate from ${new Date(start).toLocaleTimeString()} to ${new Date(end).toLocaleTimeString()}, axis upper bound ${max.toFixed(1)} commands per second`}>
    {[0, 0.5, 1].map(fraction => <g key={fraction}><line x1="45" x2="945" y1={185 - fraction * 150} y2={185 - fraction * 150} className="grid-line" /><text x="5" y={190 - fraction * 150}>{(max * fraction).toFixed(0)}</text></g>)}
    <path d={path} className="chart-line" /><text x="45" y="220">{new Date(start).toLocaleTimeString()}</text><text x="945" y="220" textAnchor="end">{new Date(end).toLocaleTimeString()}</text>
  </svg><div className="chart-caption"><span className="legend-dot" />Observed commands / second <span>· {Math.round((end - start) / 1000)}s window · browser session only</span></div></div>;
}
export function Overview() {
  const { get, history } = useRuntime();
  const data = useQuery({ queryKey: ['overview'], queryFn: ({ signal }) => get<OverviewData>('/api/overview', signal) });
  return <><PageTitle eyebrow="Instance visibility" title="Overview" description="A clear view of your running instance. Live measurements, not estimates of activity." actions={<button onClick={() => void data.refetch()}>Refresh snapshot</button>} />
    {data.isPending ? <Loading /> : data.isError ? <ErrorNotice error={data.error} retry={() => void data.refetch()} /> : (() => {
      const { metrics: m, server: s } = data.data;
      return <><div className="instance-strip"><div><span className="status-dot" /><strong>{s.name}</strong><code>v{s.version}</code></div><span>{s.storage_mode} storage</span><span>{s.shards} shards</span><span>{s.role}</span><span>{s.redis_baseline} baseline</span></div>
        <div className="metric-grid"><Metric label="Commands processed" value={number(m.commands_total)} note="Since this process started" /><Metric label="Connected clients" value={number(m.connections)} note={`${number(m.blocked_clients)} currently blocked`} /><Metric label="Keys" value={number(data.data.key_count)} note="Current database key count" /><Metric label="Process memory" value={size(m.rss_bytes)} note="Resident set size · actual process RSS" /></div>
        <Panel title="Throughput" note="Observed history · maximum 15 minutes"><ObservedChart history={history} /></Panel>
        <div className="two-column"><Panel title="Command latency" note="Since-boot histogram · approximate bucket bounds"><div className="latency-grid">{(['p50_us', 'p95_us', 'p99_us'] as const).map((key, i) => <div key={key}><span className="muted">{['P50', 'P95', 'P99'][i]}</span><strong>{m.latency[key] == null ? 'Unavailable' : `${number(m.latency[key])} µs`}</strong></div>)}</div><div className="panel-footer">{number(m.latency.samples)} recorded samples · {number(m.errors_total)} command errors</div></Panel>
          <Panel title="Durability & replication"><dl className="metadata compact"><div><dt>Uptime</dt><dd>{number(Math.floor(m.uptime_seconds))} seconds</dd></div><div><dt>Durability</dt><dd>{s.durability}</dd></div><div><dt>WAL</dt><dd>{size(m.wal.bytes)} · {m.wal.policy} · {m.wal.dirty ? 'Pending sync' : 'Clean'}</dd></div><div><dt>Replication</dt><dd>{m.replication.role} · {m.replication.status}</dd></div><div><dt>Replication lag</dt><dd>{m.replication.lag == null ? 'Unavailable' : number(m.replication.lag)}</dd></div></dl></Panel></div>
        <Panel title="Storage snapshot" note="Reported by the storage engine"><JsonView value={data.data.storage} /></Panel></>;
    })()}</>;
}
