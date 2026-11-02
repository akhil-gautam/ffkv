import { createContext, useCallback, useContext, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { useQuery, useQueryClient } from '@tanstack/react-query';
import { ApiError, request, type Catalog, type Metrics, type Overview, type Session } from './api';
import { ConfirmDialog, type Confirmation } from './components';

type Runtime = {
  session: Session;
  get: <T>(path: string, signal?: AbortSignal) => Promise<T>;
  post: <T>(path: string, body: unknown) => Promise<T>;
  confirm: (action: Confirmation) => void;
  logout: () => void;
  live: 'connecting' | 'live' | 'polling' | 'offline';
  history: Metrics[];
  lastObserved: number | null;
};
const Context = createContext<Runtime | null>(null);
export function useRuntime() { const runtime = useContext(Context); if (!runtime) throw new Error('Missing authenticated console context'); return runtime; }
export function useCatalog() { const { get } = useRuntime(); return useQuery({ queryKey: ['commands'], queryFn: ({ signal }) => get<Catalog>('/api/commands', signal), staleTime: 60_000 }); }
export function RuntimeProvider({ token, session, logout, children }: { token: string; session: Session; logout: () => void; children: ReactNode }) {
  const client = useQueryClient();
  const [action, setAction] = useState<Confirmation | null>(null);
  const [live, setLive] = useState<Runtime['live']>('connecting');
  const [history, setHistory] = useState<Metrics[]>([]);
  const [lastObserved, setLastObserved] = useState<number | null>(null);
  const wsLive = useRef(false);
  const expired = useRef(false);
  const guarded = useCallback(async <T,>(path: string, body?: unknown, signal?: AbortSignal): Promise<T> => {
    try { return await request<T>(token, path, body, signal); }
    catch (error) {
      if (error instanceof ApiError && error.status === 401 && !expired.current) { expired.current = true; logout(); }
      throw error;
    }
  }, [token, logout]);
  const observe = useCallback((data: Overview) => {
    const metric = data.metrics;
    if (!metric || !Number.isFinite(metric.observed_at_ms)) return;
    setLastObserved(Date.now());
    setHistory(previous => {
      const last = previous.at(-1);
      if (last && metric.observed_at_ms <= last.observed_at_ms) return previous;
      const restarted = last && (metric.commands_total < last.commands_total || metric.uptime_seconds < last.uptime_seconds);
      const base = restarted ? [] : previous;
      return [...base.filter(item => item.observed_at_ms >= metric.observed_at_ms - 15 * 60_000), metric].slice(-300);
    });
  }, []);
  const overview = useQuery({ queryKey: ['overview'], queryFn: async ({ signal }) => {
    try { const data = await guarded<Overview>('/api/overview', undefined, signal); if (!wsLive.current) setLive('polling'); observe(data); return data; }
    catch (error) { if (!wsLive.current) setLive('offline'); throw error; }
  }, refetchInterval: live === 'live' ? 30_000 : 5_000 });
  useEffect(() => {
    let stopped = false; let socket: WebSocket | null = null; let reconnect: number | undefined;
    let watchdog: number | undefined; let attempts = 0; let lastMessage = 0;
    const connect = () => {
      if (stopped) return;
      setLive('connecting');
      socket = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/api/live`);
      socket.onopen = () => { socket?.send(JSON.stringify({ token })); lastMessage = Date.now(); };
      socket.onmessage = event => {
        if (stopped) return;
        try {
          const data = JSON.parse(event.data) as Overview;
          if (!data.server || !data.metrics) return;
          attempts = 0; lastMessage = Date.now(); wsLive.current = true; setLive('live');
          client.setQueryData(['overview'], data); observe(data);
        } catch { /* Invalid live data does not replace the last valid REST snapshot. */ }
      };
      socket.onerror = () => socket?.close();
      socket.onclose = event => {
        wsLive.current = false;
        if (stopped) return;
        if (event.code === 1008 || event.code === 4401) { void guarded<Session>('/api/session').catch(() => {}); }
        setLive('polling'); void client.invalidateQueries({ queryKey: ['overview'] });
        reconnect = window.setTimeout(connect, Math.min(30_000, 1000 * 2 ** Math.min(attempts++, 5)));
      };
    };
    connect();
    watchdog = window.setInterval(() => { if (lastMessage && Date.now() - lastMessage > 20_000) socket?.close(); }, 5_000);
    return () => { stopped = true; clearTimeout(reconnect); clearInterval(watchdog); socket?.close(); wsLive.current = false; };
  }, [token, client, guarded, observe]);
  // Keep the query mounted across navigation so REST is a real fallback to WS.
  void overview;
  const value = useMemo<Runtime>(() => ({ session, get: (path, signal) => guarded(path, undefined, signal), post: (path, body) => guarded(path, body), confirm: setAction, logout, live, history, lastObserved }), [session, guarded, logout, live, history, lastObserved]);
  return <Context.Provider value={value}>{children}<ConfirmDialog action={action} close={() => setAction(null)} /></Context.Provider>;
}
