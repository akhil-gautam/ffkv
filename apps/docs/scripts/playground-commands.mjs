import { readFile } from 'node:fs/promises';
import path from 'node:path';

// Which canonical commands the browser playground answers with "not available",
// read from its dispatch source (src/wasm/session.rs). playground-smoke.mjs checks
// this against the built engine for every catalog command.
export async function playgroundRefuses(root) {
  const source = await readFile(path.join(root, 'src/wasm/session.rs'), 'utf8');
  const list = (name) => {
    const match = source.match(new RegExp(`pub\\(crate\\) const ${name}: &\\[&str\\] = &\\[([^\\]]*)\\];`));
    if (!match) throw new Error(`src/wasm/session.rs no longer declares the ${name} command list`);
    return new Set([...match[1].matchAll(/"([^"]+)"/g)].map((item) => item[1].toUpperCase()));
  };
  const unavailable = list('UNAVAILABLE');
  const client = list('CLIENT_AVAILABLE');
  return (name) => {
    const [command, subcommand] = name.split(' ');
    return unavailable.has(command) || (command === 'CLIENT' && subcommand !== undefined && !client.has(subcommand));
  };
}

/**
 * Run lines in order on a fresh dataset and connection and return the last command's
 * output, or null unless every reply is immediate and not an error and the example
 * actually runs rather than waiting in an open MULTI.
 */
function replayOutput(playground, lines) {
  playground.reset();
  const session = playground.open();
  try {
    let last;
    for (const line of lines) {
      last = session.eval(line);
      if (last.error || last.blocked) return null;
    }
    return last.output === 'QUEUED\n' ? null : last.output;
  } finally {
    session.close();
  }
}

/**
 * The sequence with every setup step it does not need removed, plus the example's
 * output, or null when it does not replay cleanly. Greedy: drop the earliest removable
 * setup step, repeat until none can go. A removal stays only if the replay is still
 * clean and the example's output is byte-identical to the full sequence's.
 */
export function minimizeSequence(playground, lines) {
  const output = replayOutput(playground, lines);
  if (output === null) return null;
  let current = lines;
  for (let index = 0; index < current.length - 1;) {
    const trial = current.toSpliced(index, 1);
    if (replayOutput(playground, trial) === output) {
      current = trial;
      index = 0;
    } else {
      index += 1;
    }
  }
  return { lines: current, output };
}
