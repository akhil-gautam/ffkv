import { StrictMode, useCallback, useEffect, useState, type FormEvent } from 'react';
import { createRoot } from 'react-dom/client';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { ApiError, request, type Session } from './api';
import { ErrorNotice } from './components';
import { RuntimeProvider, useRuntime } from './runtime';
import { Overview } from './Overview';
import { Keys } from './Keys';
import { Queues } from './Queues';
import { Commands } from './Commands';
import { Audit, Compatibility, Storage } from './Operations';
import './styles.css';

const client = new QueryClient({ defaultOptions: { queries: { retry: (attempt, error) => !(error instanceof ApiError && [400, 401, 403, 404, 429].includes(error.status)) && attempt < 2, staleTime: 2000, refetchOnWindowFocus: true }, mutations: { retry: false } } });
const pages = [
  { id: 'overview', label: 'Overview', icon: 'overview', group: 'Observe' },
  { id: 'queues', label: 'Queues', icon: 'queues', group: 'Observe' },
  { id: 'keys', label: 'Key explorer', icon: 'keys', group: 'Operate' },
  { id: 'commands', label: 'Command console', icon: 'commands', group: 'Operate' },
  { id: 'compatibility', label: 'Compatibility', icon: 'compatibility', group: 'Manage' },
  { id: 'storage', label: 'Storage & recovery', icon: 'storage', group: 'Manage' },
  { id: 'audit', label: 'Audit trail', icon: 'audit', group: 'Manage' },
];
function Icon({ name }: { name: string }) {
  const paths: Record<string, string> = {
    overview: 'M3 3h7v7H3z M14 3h7v7h-7z M3 14h7v7H3z M14 14h7v7h-7z',
    queues: 'M4 5h16 M4 12h12 M4 19h8 M19 15l3 4-3 4',
    keys: 'M15 14a6 6 0 1 0-4-10 6 6 0 0 0 4 10z M11 13l-8 8 M4 17l3 3 M7 14l3 3',
    commands: 'M4 6l6 6-6 6 M13 18h7',
    compatibility: 'M9 4H4v16h5 M15 4h5v16h-5 M8 12l3 3 5-6',
    storage: 'M4 5c0-4 16-4 16 0s-16 4-16 0v14c0 4 16 4 16 0V5 M4 12c0 4 16 4 16 0',
    audit: 'M6 3h12v18H6z M9 7h6 M9 11h6 M9 15h4',
  };
  return <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" aria-hidden="true"><path d={paths[name] || paths.overview} /></svg>;
}
function Brand() { return <div className="brand"><span className="brand-mark" aria-hidden="true">f<span>f</span></span><div><strong>ffkv</strong><span>freaking fast KV</span></div></div>; }
function Login({ enter }: { enter: (token: string, session: Session) => void }) {
  const [token, setToken] = useState(''); const [busy, setBusy] = useState(false); const [error, setError] = useState<unknown>(null);
  const submit = async (e: FormEvent) => {
    e.preventDefault(); if (!token.trim() || busy) return; setBusy(true); setError(null);
    try { const secret = token.trim(); const session = await request<Session>(secret, '/api/session'); setToken(''); enter(secret, session); }
    catch (e) { setError(e); } finally { setBusy(false); }
  };
  return <main className="login-layout"><div className="login-brand"><Brand /><span className="eyebrow">Infrastructure console</span></div><div className="login-body"><div className="login-intro"><div className="eyebrow">Direct visibility. Deliberate control.</div><h1>Small footprint.<br />Clear operations.</h1><p>Inspect your data, understand your queues, and operate with confidence. A focused control plane for ffkv.</p><div className="login-facts"><span>Binary-safe inspection</span><span>Audited operations</span><span>Read-only by default</span></div></div><section className="login-card"><div className="eyebrow">Secure management access</div><h2>Connect to your instance</h2><p>Enter a management bearer token or a valid OIDC access token issued for this server.</p><form onSubmit={submit}><label className="field"><span>Access token</span><input autoFocus type="password" autoComplete="off" autoCapitalize="none" spellCheck={false} value={token} onChange={e => setToken(e.target.value)} required aria-describedby="token-policy" /></label>{error != null && <ErrorNotice error={error} />}<button type="submit" className="primary" disabled={busy || !token.trim()}>{busy ? 'Authenticating…' : 'Open console'}<span aria-hidden="true">→</span></button></form><p id="token-policy" className="help">Your token stays in memory only. Nothing is written to browser storage or placed in a URL. Closing or refreshing this page ends the local session.</p><div className="login-endpoint"><span className="status-dot neutral" />Same-origin management API <code>{location.host}</code></div></section></div><footer className="login-footer">ffkv management plane<span>Use HTTPS outside a trusted loopback environment.</span></footer></main>;
}
function Shell() {
  const { session, logout, live, lastObserved } = useRuntime(); const [page, setPage] = useState(() => location.hash.slice(1) || 'overview'); const [menu, setMenu] = useState(false); const [now, setNow] = useState(Date.now());
  useEffect(() => { const change = () => { setPage(location.hash.slice(1) || 'overview'); setMenu(false); }; addEventListener('hashchange', change); const timer = window.setInterval(() => setNow(Date.now()), 1000); return () => { removeEventListener('hashchange', change); clearInterval(timer); }; }, []);
  const valid = pages.some(p => p.id === page); const current = pages.find(p => p.id === page);
  const stale = lastObserved != null && now - lastObserved > 20_000;
  const status = stale ? 'Stale snapshot' : live === 'live' ? 'Live stream' : live === 'polling' ? 'REST polling' : live === 'offline' ? 'Disconnected' : 'Connecting';
  return <div className="app-shell"><a className="skip-link" href="#main-content" onClick={e => { e.preventDefault(); document.getElementById('main-content')?.focus(); }}>Skip to content</a><aside className={`sidebar ${menu ? 'open' : ''}`}><Brand /><div className="workspace-label"><span className="status-dot" />Management plane</div><nav aria-label="Primary navigation">{pages.map((item, index) => <div key={item.id}>{(index === 0 || pages[index - 1].group !== item.group) && <div className="nav-group">{item.group}</div>}<a href={`#${item.id}`} className={page === item.id ? 'active' : ''} aria-current={page === item.id ? 'page' : undefined}><Icon name={item.icon} /><span>{item.label}</span>{item.id === 'audit' && session.role !== 'admin' && <span className="nav-lock" aria-label="Admin only">A</span>}</a></div>)}</nav><div className="sidebar-foot"><div className="avatar">{session.name.slice(0, 1).toUpperCase()}</div><div><strong>{session.name}</strong><span>{session.role} session</span></div><button className="logout" aria-label="Log out and clear session" onClick={logout}>↗</button></div></aside>{menu && <button className="menu-scrim" aria-label="Close navigation" onClick={() => setMenu(false)} />}
    <div className="main-shell"><header className="topbar"><button className="menu-toggle" aria-expanded={menu} aria-label="Toggle navigation" onClick={() => setMenu(v => !v)}>☰</button><div className="breadcrumb">Console <span>/</span> <strong>{current?.label || 'Not found'}</strong></div><div className="connection" role="status"><span className={`status-dot ${stale || live === 'offline' ? 'bad' : live === 'connecting' ? 'neutral' : ''}`} /><span>{status}</span>{lastObserved && <time title={new Date(lastObserved).toLocaleString()}>{Math.max(0, Math.floor((now - lastObserved) / 1000))}s ago</time>}</div><span className="role-badge">{session.role === 'viewer' ? 'Read-only access' : 'Administrator'}</span></header>
      <main id="main-content" tabIndex={-1}>{stale && <div className="notice error" role="alert">The last valid measurement is over 20 seconds old. Displayed values are not current.</div>}{page === 'overview' ? <Overview /> : page === 'queues' ? <Queues /> : page === 'keys' ? <Keys /> : page === 'commands' ? <Commands /> : page === 'compatibility' ? <Compatibility /> : page === 'storage' ? <Storage /> : page === 'audit' ? <Audit /> : !valid ? <section className="panel"><h1>Page not found</h1><a href="#overview">Return to overview</a></section> : null}<footer className="content-footer"><span>ffkv / infrastructure console</span><span>Safe inspection. Explicit changes.</span></footer></main>
    </div></div>;
}
function App() {
  const [auth, setAuth] = useState<{ token: string; session: Session } | null>(null);
  const logout = useCallback(() => { setAuth(null); void client.cancelQueries(); client.clear(); }, []);
  return auth ? <RuntimeProvider token={auth.token} session={auth.session} logout={logout}><Shell /></RuntimeProvider> : <Login enter={(token, session) => { client.clear(); setAuth({ token, session }); }} />;
}

createRoot(document.getElementById('root')!).render(<StrictMode><QueryClientProvider client={client}><App /></QueryClientProvider></StrictMode>);
