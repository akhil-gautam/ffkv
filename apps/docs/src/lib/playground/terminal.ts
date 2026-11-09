// Interactive terminals for the browser playground. Every terminal on a page is
// one client session of the same in-tab engine, so they share one dataset.
import { Playground, type Reply, type Session } from './ffkv';

const DEFAULT_PROMPT = 'ffkv> ';
const HISTORY_LIMIT = 200;

let shared: Promise<Playground> | undefined;
let loaded: Playground | undefined;
const terminals = new Map<string, Terminal>();
let timer: number | undefined;

/** Load the engine once per page; a failed load is retried on the next interaction. */
function loadPlayground(url: string): Promise<Playground> {
  shared ??= Playground.load(url).then(
    (playground) => (loaded = playground),
    (error: unknown) => {
      shared = undefined;
      throw error;
    },
  );
  return shared;
}

/** Deliver output that became ready in any session; keep polling while one is blocked. */
function pollAll(): void {
  for (const terminal of terminals.values()) terminal.poll();
  const waiting = [...terminals.values()].some((terminal) => terminal.blocked);
  if (waiting && !timer) timer = window.setInterval(pollAll, 100);
  if (!waiting && timer) {
    clearInterval(timer);
    timer = undefined;
  }
}

class Terminal {
  readonly name: string;
  blocked = false;
  readonly #root: HTMLElement;
  readonly #screen: HTMLElement;
  readonly #log: HTMLElement;
  readonly #form: HTMLFormElement;
  readonly #input: HTMLInputElement;
  readonly #prompt: HTMLElement;
  readonly #status: HTMLElement;
  readonly #waiting: HTMLElement;
  readonly #pendingRow: HTMLElement;
  readonly #wasm: string;
  #session: Session | undefined;
  #connecting: Promise<Session> | undefined;
  #busy = false;
  readonly #history: string[] = [];
  #cursor = 0;
  #draft = '';
  /** Lines a "Try in playground" link asked for, run in order on Enter or Run all. */
  #pending: string[] = [];

  constructor(root: HTMLElement) {
    const part = <T extends HTMLElement>(selector: string) => root.querySelector<T>(selector)!;
    this.name = root.dataset.ffkvTerminal!;
    this.#root = root;
    this.#screen = part('[data-screen]');
    this.#log = part('[data-log]');
    this.#form = part<HTMLFormElement>('[data-form]');
    this.#input = part<HTMLInputElement>('[data-input]');
    this.#prompt = part('[data-prompt]');
    this.#status = part('[data-status]');
    this.#waiting = part('[data-waiting]');
    this.#pendingRow = part('[data-pending]');
    this.#wasm = root.dataset.wasm!;

    this.#form.addEventListener('submit', (event) => {
      event.preventDefault();
      void this.submit(this.#input.value);
    });
    root.addEventListener('keydown', (event) => this.#key(event));
    part('[data-clear]').addEventListener('click', () => this.clear());
    part('[data-cancel]').addEventListener('click', () => this.cancel());
    part('[data-run-pending]').addEventListener('click', () => void this.submit(this.#pending[0], this.#pending));
    // Clicking anywhere in the screen types into the prompt, unless the reader is selecting output.
    this.#screen.addEventListener('click', () => {
      if (document.getSelection()?.isCollapsed !== false) this.#input.focus({ preventScroll: true });
    });
    // Load lazily: on first interaction here, or when the terminal approaches the viewport (see mountTerminals).
    root.addEventListener('focusin', () => void this.connect().catch(() => {}), { once: true });

    // ?cmd= may repeat: setup commands from a test scenario, then the example itself.
    const linked = root.hasAttribute('data-query-prefill') ? new URL(location.href).searchParams.getAll('cmd') : [];
    if (linked.length) {
      this.#input.value = linked[0];
      this.#setPending(linked);
      void this.connect().catch(() => {});
    }
  }

  /** Open this terminal's session, loading the engine first if needed. */
  connect(): Promise<Session> {
    if (this.#session && !this.#session.closed) return Promise.resolve(this.#session);
    this.#connecting ??= (async () => {
      this.#setStatus('loading', 'Loading engine…');
      try {
        const playground = await loadPlayground(this.#wasm);
        this.#session = playground.open();
        this.#setStatus('ready', `client ${this.#session.id}`);
        this.note(`Connected as client ${this.#session.id}. Data lives in this tab's memory only.`);
        return this.#session;
      } catch (error) {
        this.#setStatus('failed', 'Not loaded');
        this.note(`Could not load the playground engine: ${error instanceof Error ? error.message : String(error)}. Press Enter to try again.`, 'error');
        throw error;
      } finally {
        this.#connecting = undefined;
      }
    })();
    return this.#connecting;
  }

  /**
   * Echo and run command lines in order, delivering output for every session after each.
   * Enter on the untouched first line of a linked sequence runs the whole sequence.
   */
  async submit(typed: string, sequence = typed === this.#pending[0] ? this.#pending : [typed]): Promise<void> {
    if (this.#busy || this.blocked) return;
    const linked = this.#pending;
    this.#setPending([]);
    this.#busy = true;
    this.#input.value = '';
    let session: Session | undefined;
    try {
      // Connect first so the connection note precedes the first command.
      if (sequence.some((line) => line.trim())) session = await this.connect();
    } catch {
      // connect() already reported the failure; keep the input so Enter retries it.
      this.#input.value = typed;
      this.#setPending(linked);
      this.#busy = false;
      return;
    }
    try {
      for (const [index, line] of sequence.entries()) {
        this.#echo(line);
        if (line.trim() && this.#history.at(-1) !== line) this.#history.push(line);
        if (this.#history.length > HISTORY_LIMIT) this.#history.shift();
        if (!session || !line.trim()) continue;
        this.show(session.eval(line));
        pollAll();
        if (this.blocked && index < sequence.length - 1) {
          this.note(`Stopped here because the command is blocking; ${sequence.length - index - 1} linked command(s) did not run.`);
          break;
        }
      }
    } catch (error) {
      this.note(`The engine failed: ${error instanceof Error ? error.message : String(error)}. Reload the page to start over.`, 'error');
    } finally {
      this.#cursor = this.#history.length;
      this.#draft = '';
      this.#busy = false;
    }
  }

  /** Run a command from a guided example button. */
  run(command: string): void {
    this.#root.scrollIntoView({ block: 'nearest', behavior: matchMedia('(prefers-reduced-motion: reduce)').matches ? 'auto' : 'smooth' });
    if (this.blocked) {
      this.note('This terminal is waiting on a blocking command. Press Cancel (or Ctrl+C) first.');
      return;
    }
    void this.submit(command);
  }

  poll(): void {
    if (!this.#session || this.#session.closed) return;
    this.show(this.#session.poll());
  }

  show(reply: Reply): void {
    if (reply.output) this.#append('out', reply.output.replace(/\n$/, ''), reply.error ? 'error' : undefined);
    this.#prompt.textContent = reply.prompt;
    this.#setBlocked(reply.blocked);
  }

  /** Abandon a blocked command the way a CLI user would: drop the connection and open a new one. */
  cancel(): void {
    if (!this.blocked || !this.#session || !loaded) return;
    this.#session.close();
    this.#session = loaded.open();
    this.#append('note', '^C');
    this.note(`Cancelled. The blocked command was dropped and a new connection opened as client ${this.#session.id} (database 0, RESP2).`);
    this.#setStatus('ready', `client ${this.#session.id}`);
    this.#prompt.textContent = DEFAULT_PROMPT;
    this.#setBlocked(false);
    pollAll();
    this.#input.focus({ preventScroll: true });
  }

  clear(): void {
    this.#log.replaceChildren();
    this.#input.focus({ preventScroll: true });
  }

  note(text: string, tone?: 'error'): void {
    this.#append('note', text, tone);
  }

  #key(event: KeyboardEvent): void {
    const control = event.ctrlKey && !event.altKey && !event.metaKey ? event.key.toLowerCase() : '';
    const inInput = event.target === this.#input;
    // Ctrl+C still copies when the reader has selected text.
    const nothingSelected = inInput ? this.#input.selectionStart === this.#input.selectionEnd : document.getSelection()?.isCollapsed !== false;
    if (control === 'l') {
      event.preventDefault();
      this.clear();
    } else if (control === 'c' && nothingSelected) {
      event.preventDefault();
      if (this.blocked) return this.cancel();
      if (!inInput) return;
      this.#echo(`${this.#input.value}^C`);
      this.#input.value = '';
      this.#cursor = this.#history.length;
    } else if (inInput && (event.key === 'ArrowUp' || event.key === 'ArrowDown') && !event.altKey && !event.shiftKey) {
      const step = event.key === 'ArrowUp' ? -1 : 1;
      const next = Math.min(this.#history.length, Math.max(0, this.#cursor + step));
      if (next === this.#cursor) return;
      event.preventDefault();
      if (this.#cursor === this.#history.length) this.#draft = this.#input.value;
      this.#cursor = next;
      this.#input.value = next === this.#history.length ? this.#draft : this.#history[next];
      const end = this.#input.value.length;
      requestAnimationFrame(() => this.#input.setSelectionRange(end, end));
    }
  }

  #echo(line: string): void {
    const entry = document.createElement('div');
    entry.className = 'ffkv-term__echo';
    const prompt = document.createElement('span');
    prompt.className = 'ffkv-term__prompt';
    prompt.textContent = this.#prompt.textContent;
    entry.append(prompt, line);
    this.#push(entry);
  }

  #append(kind: 'out' | 'note', text: string, tone?: 'error'): void {
    const entry = document.createElement('div');
    entry.className = `ffkv-term__${kind}${tone ? ` is-${tone}` : ''}`;
    entry.textContent = text;
    this.#push(entry);
  }

  #push(entry: HTMLElement): void {
    this.#log.append(entry);
    this.#screen.scrollTop = this.#screen.scrollHeight;
  }

  #setBlocked(blocked: boolean): void {
    if (blocked === this.blocked) return;
    this.blocked = blocked;
    // Keep keyboard focus inside the terminal when the input and Cancel swap places.
    const focused = this.#root.contains(document.activeElement);
    this.#waiting.hidden = !blocked;
    this.#form.hidden = blocked;
    if (focused) (blocked ? this.#waiting.querySelector('button')! : this.#input).focus({ preventScroll: true });
    this.#screen.scrollTop = this.#screen.scrollHeight;
  }

  #setStatus(state: 'loading' | 'ready' | 'failed', text: string): void {
    this.#root.dataset.state = state;
    this.#status.textContent = text;
  }

  #setPending(lines: string[]): void {
    this.#pending = lines;
    this.#pendingRow.hidden = lines.length < 2;
    this.#pendingRow.querySelector('span')!.textContent = `This link runs ${lines.length} commands from a test scenario: setup first, the example last. Press Enter to run them in order.`;
  }
}

function mountTerminals(): void {
  // One observer for all terminals reports them in page order, so client ids follow the layout.
  const near = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (!entry.isIntersecting) continue;
      near.unobserve(entry.target);
      void terminals.get((entry.target as HTMLElement).dataset.ffkvTerminal!)?.connect().catch(() => {});
    }
  }, { rootMargin: '200px' });
  for (const root of document.querySelectorAll<HTMLElement>('[data-ffkv-terminal]:not([data-mounted])')) {
    root.dataset.mounted = '';
    const terminal = new Terminal(root);
    terminals.set(terminal.name, terminal);
    near.observe(root);
  }
}

// Guided example buttons: <button data-ffkv-run="a" data-command="SET k v">.
document.addEventListener('click', (event) => {
  const button = (event.target as Element | null)?.closest<HTMLElement>('[data-ffkv-run]');
  if (!button) return;
  terminals.get(button.dataset.ffkvRun!)?.run(button.dataset.command!);
});

// Reset buttons empty the shared dataset. Before the engine loads there is nothing to reset.
document.addEventListener('click', (event) => {
  if (!(event.target as Element | null)?.closest('[data-ffkv-reset]') || !loaded) return;
  loaded.reset();
  for (const terminal of terminals.values()) terminal.note('Playground reset: every database is now empty.');
});

mountTerminals();
