import { readdir, readFile } from 'node:fs/promises';
import path from 'node:path';

// Read JSON docstrings attached to known command steps, never arbitrary bracketed
// prose/assertions. This intentionally does not execute Gherkin or Python hooks.
const commandStep = /^(?:I execute:|client "[^"]+"(?: on node "[^"]+")? (?:executes|sends):|node "[^"]+" executes:)$/;
const batchStep = /^the following commands succeed:$/;
const stepPattern = /^\s*(?:Given|When|Then|And|But|\*)\s+(.+)$/;

function tableCells(line) {
  return line.trim().slice(1, -1).split(/(?<!\\)\|/).map(cell => cell.trim().replace(/\\\|/g, '|').replace(/\\n/g, '\n').replace(/\\\\/g, '\\'));
}
function substitute(text, row) {
  return text.replace(/<([^<>]+)>/g, (match, name) => row[name] ?? match);
}
function stepsFrom(lines, firstLine) {
  const steps = [];
  for (let i = 0; i < lines.length; i++) {
    const match = lines[i].match(stepPattern);
    if (!match) continue;
    const step = { text: match[1], line: firstLine + i, document: null };
    if (/^\s*(?:"""|```)/.test(lines[i + 1] ?? '')) {
      const delimiter = lines[++i].trim().slice(0, 3);
      const start = ++i;
      while (i < lines.length && !lines[i].trim().startsWith(delimiter)) i++;
      if (i === lines.length) throw new Error(`Unclosed BDD docstring at line ${firstLine + start}`);
      step.document = lines.slice(start, i).join('\n');
    }
    steps.push(step);
  }
  return steps;
}
function exampleRows(lines) {
  const rows = [];
  let headers = null;
  let reading = false;
  for (const line of lines) {
    if (/^\s*Examples:/.test(line)) { reading = true; headers = null; continue; }
    if (!reading || !/^\s*\|.*\|\s*$/.test(line)) continue;
    const cells = tableCells(line);
    if (!headers) { headers = cells; continue; }
    if (cells.length !== headers.length) throw new Error(`Malformed BDD examples row: ${line}`);
    rows.push(Object.fromEntries(headers.map((header, index) => [header, cells[index]])));
  }
  return rows;
}
function parseDocument(text, location, parameterized) {
  try { return JSON.parse(text); }
  catch (error) {
    // Outline substitutions may intentionally be fragments understood by Behave;
    // never turn an unresolved template into a fabricated executable invocation.
    if (parameterized) return null;
    throw new Error(`Invalid command JSON in ${location}: ${error.message}`);
  }
}
function vectorName(argv, names) {
  if (!Array.isArray(argv) || typeof argv[0] !== 'string') return null;
  const tokens = argv.slice(0, 3).map(value => typeof value === 'string' ? value.toUpperCase() : '');
  for (let length = tokens.length; length > 0; length--) {
    const name = tokens.slice(0, length).join(' ');
    if (names.has(name)) return name;
  }
  return null;
}
export async function collectBdd(root, names) {
  const directory = path.join(root, 'tests/bdd/features');
  const coverage = new Map([...names].map(name => [name, { scenarios: [], examples: [] }]));
  const tags = new Map([...names].map(name => [`command_${name.replaceAll(' ', '_').replaceAll('.', '_').replaceAll('-', '_')}`, name]));
  const files = (await readdir(directory)).filter(file => file.endsWith('.feature')).sort();
  if (!files.length) throw new Error('No BDD feature files found');
  for (const file of files) {
    const relative = `tests/bdd/features/${file}`;
    const lines = (await readFile(path.join(directory, file), 'utf8')).split(/\r?\n/);
    let pendingTags = [];
    for (let i = 0; i < lines.length; i++) {
      if (/^\s*@/.test(lines[i])) { pendingTags.push(...lines[i].trim().split(/\s+/).map(tag => tag.slice(1))); continue; }
      const match = lines[i].match(/^\s*Scenario( Outline)?:\s*(.+)$/);
      if (!match) continue;
      const scenarioLine = i + 1;
      let end = i + 1;
      while (end < lines.length && !/^\s*(?:Scenario(?: Outline)?:|@)/.test(lines[end])) end++;
      const body = lines.slice(i + 1, end);
      const steps = stepsFrom(body, i + 2);
      const outline = Boolean(match[1]);
      const rows = outline ? exampleRows(body) : [{}];
      const scenario = { name: match[2], file: relative, line: scenarioLine, outline };
      const taggedNames = pendingTags.map(tag => tags.get(tag)).filter(Boolean);
      const observed = new Set();
      for (const row of rows) {
        for (let s = 0; s < steps.length; s++) {
          const step = steps[s];
          const text = substitute(step.text, row);
          if ((!commandStep.test(text) && !batchStep.test(text)) || step.document === null) continue;
          const document = parseDocument(substitute(step.document, row), `${relative}:${step.line}`, outline);
          if (document === null) continue;
          const vectors = batchStep.test(text) ? document : [document];
          if (!Array.isArray(vectors)) throw new Error(`Expected command list at ${relative}:${step.line}`);
          for (const argv of vectors) {
            const name = vectorName(argv, names);
            if (!name) continue;
            observed.add(name);
            const next = batchStep.test(text) ? null : steps[s + 1];
            let expectation = null;
            if (next && /^(?:the reply |client "[^"]+" receives:)/.test(next.text)) {
              expectation = { text: substitute(next.text, row), document: next.document === null ? null : substitute(next.document, row).trim() };
            }
            coverage.get(name).examples.push({ ...scenario, line: step.line, argv, step: text, parameters: outline ? row : null, expectation, fixtureDependent: /\{\{[^}]+\}\}/.test(JSON.stringify(argv)), binary: argv.some(value => typeof value === 'object' && value !== null && 'base64' in value) });
          }
        }
      }
      for (const name of new Set([...taggedNames, ...observed])) {
        coverage.get(name).scenarios.push({ ...scenario, tagged: taggedNames.includes(name), observed: observed.has(name) });
      }
      pendingTags = [];
      i = end - 1;
    }
  }
  return coverage;
}
