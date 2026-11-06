#!/usr/bin/env node
/**
 * ============================================================================
 *  POWER AGENT v4 : one-file, fully autonomous, UNLIMITED coding agent
 *  (Node.js 20+, zero npm dependencies)
 * ============================================================================
 *  EDITING    apply_patch (fuzzy search/replace, multi-file, atomic rollback),
 *             multi_edit, create_file, delete, move, batch_read, unified diff
 *  NAVIGATION grep, glob, tree, repo_map, symbol_find, read_file w/ ranges,
 *             project memory (POWERAGENT.md)
 *  WORKFLOW   todo_write, plan mode, subagents, parallel fan-out, run_tests,
 *             screenshot (headless chrome), run_python, deps_audit, port_check
 *  SAFETY     checkpoints + /undo + rewind, permission modes, command denylist,
 *             workspace path jail
 *  GIT        status/diff/log/commit/branch/stash/checkout
 *  EXTEND     MCP stdio client, after-edit hooks, model fallback
 *  CONTEXT    adaptive token budget (learns the model limit), auto-compaction, streaming
 *  AUTONOMY   auto-allow by default, unlimited steps, auto-continue on cut-off replies,
 *             retry/backoff + model failover, loop detection, todo auto-continue,
 *             parallel subagents, 1h shell timeout
 *  UI         Claude-style chat (dark/light, serif replies, collapsible tool steps, artifact panel
 *             that RUNS generated HTML/video, file manager, mobile-first, drag-drop + paste)
 *  CAPABILITY env_info, install_package (apt/pkg/brew/pip/npm + auto headless Chromium), http_request,
 *             define_tool (self-made tools), mcp_add, live preview server (own origin), Range video
 *             composer attachments (button + drag-drop + paste)
 *
 *  Run:      node ai.js          (Termux, laptop, VPS, Docker, Render, Railway, Fly.io, Replit, Codespaces, HF Spaces...)
 *  Update:   node ai.js --update-only
 *  Needs:    Node.js 20+ and internet. git + npm only when FreeLLMAPI is used (the default).
 *
 *  HOSTING   PORT, 0.0.0.0, https cookies and proxy headers are picked up automatically. Plain environment
 *            variables (hosting dashboards, docker -e) always beat the .env file:
 *              AGENT_PASSWORD     login password. SET IT: otherwise a new one is generated whenever the disk is fresh
 *              POWER_AGENT_HOME   persistent folder (a mounted volume) for chats, files, settings
 *              LLM_BASE_URL / LLM_API_KEY / LLM_MODEL
 *                                 use ANY OpenAI-compatible API (OpenRouter, Groq, OpenAI...) and skip FreeLLMAPI
 *                                 completely (recommended on small / free hosts: no git, no build, instant start)
 *              PUBLIC_URL, PREVIEW_MODE (port|inline|off), SELF_PING_MINUTES, AGENT_HOST, AGENT_PORT, ALLOWED_ORIGINS
 * ============================================================================
 */
// Wrapped in an async function with dynamic imports so the SAME file runs as CommonJS or as an ES module,
// on any Node 20+, with or without a package.json (hosting platforms generate those in all shapes).
(async () => {
'use strict';
const { spawn, exec, spawnSync } = await import('node:child_process');
const crypto = (await import('node:crypto')).default;
const fs = (await import('node:fs')).default;
const http = (await import('node:http')).default;
const net = (await import('node:net')).default;
const os = (await import('node:os')).default;
const { Readable } = await import('node:stream');
const { pipeline } = await import('node:stream/promises');
const path = (await import('node:path')).default;

// ============================================================================
// 0. CONFIG
// ============================================================================
const ROOT = (() => { try { return path.dirname(fs.realpathSync(process.argv[1])); } catch { return process.cwd(); } })();
const IS_WIN = process.platform === 'win32';
const penv = process.env;

// ---- where are we running? (Termux, laptop, Docker, Render, Railway, Fly, Replit, Codespaces, ...) ----
const IN_DOCKER = (() => { try { return fs.existsSync('/.dockerenv') || /docker|kubepods|containerd/.test(fs.readFileSync('/proc/1/cgroup', 'utf8')); } catch { return false; } })();
const IS_TERMUX = !!penv.TERMUX_VERSION || fs.existsSync('/data/data/com.termux');
const PLATFORM_TABLE = [
  ['Render', penv.RENDER], ['Railway', penv.RAILWAY_ENVIRONMENT || penv.RAILWAY_PROJECT_ID], ['Fly.io', penv.FLY_APP_NAME],
  ['Replit', penv.REPL_ID], ['Heroku', penv.DYNO], ['Koyeb', penv.KOYEB_APP_NAME], ['Cloud Run', penv.K_SERVICE],
  ['Hugging Face Space', penv.SPACE_ID], ['GitHub Codespaces', penv.CODESPACES], ['Gitpod', penv.GITPOD_WORKSPACE_ID],
];
const PLATFORM = (PLATFORM_TABLE.find(([, v]) => v) || [])[0] || (IS_TERMUX ? 'Termux' : IN_DOCKER ? 'Docker' : '');
// "hosted" = a platform tells us which PORT to use / we are in a container: bind 0.0.0.0, single public port, https behind a proxy
const HOSTED = !IS_TERMUX && (!!penv.PORT || !!PLATFORM || IN_DOCKER);

const canWrite = (d) => { try { fs.mkdirSync(d, { recursive: true }); fs.accessSync(d, fs.constants.W_OK); return true; } catch { return false; } };
function pickHome() {
  if (penv.POWER_AGENT_HOME) {                                   // explicit: a mounted volume
    if (canWrite(path.resolve(penv.POWER_AGENT_HOME))) return path.resolve(penv.POWER_AGENT_HOME);
    console.warn(`POWER_AGENT_HOME=${penv.POWER_AGENT_HOME} is not writable, falling back.`);
  }
  const legacy = path.join(ROOT, '.power-agent');
  if (fs.existsSync(legacy) && canWrite(legacy)) return legacy;  // existing installs keep their data
  let userHome = ROOT; try { userHome = os.homedir(); } catch { /* no passwd entry */ }
  const list = [
    penv.RAILWAY_VOLUME_MOUNT_PATH && path.join(penv.RAILWAY_VOLUME_MOUNT_PATH, 'power-agent'),
    HOSTED && fs.existsSync('/data') && '/data/power-agent',     // Fly volume / HF persistent storage / Docker volume
    HOSTED ? path.join(userHome, '.power-agent') : legacy,       // keep the workspace out of the app folder when hosted
    legacy, path.join(userHome, '.power-agent'), path.join(os.tmpdir(), 'power-agent'),
  ].filter(Boolean);
  return list.find(canWrite) || legacy;
}
const HOME = pickHome();
const ENV_FILE = path.join(HOME, '.env');
const DATA = path.join(HOME, 'data');
fs.mkdirSync(DATA, { recursive: true });

function parseEnv(text) {
  const out = {};
  for (const raw of text.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#') || !line.includes('=')) continue;
    const i = line.indexOf('=');
    let v = line.slice(i + 1).trim();
    if (/^(["']).*\1$/.test(v)) v = v.slice(1, -1);
    out[line.slice(0, i).trim()] = v;
  }
  return out;
}
const readEnv = (f) => { try { return parseEnv(fs.readFileSync(f, 'utf8')); } catch { return {}; } };
function upsertEnv(file, updates) {
  let lines = [];
  try { lines = fs.readFileSync(file, 'utf8').split(/\r?\n/); } catch { /* new */ }
  const done = new Set();
  lines = lines.map((l) => { const m = l.match(/^\s*([A-Za-z_]\w*)\s*=/); if (m && m[1] in updates) { done.add(m[1]); return `${m[1]}=${updates[m[1]]}`; } return l; });
  for (const [k, v] of Object.entries(updates)) if (!done.has(k)) lines.push(`${k}=${v}`);
  fs.writeFileSync(file, lines.join('\n').replace(/\n*$/, '\n'), { mode: 0o600 });
}

const ENV_TEMPLATE = `# Power Agent config (auto-created). Restart after editing.
# Real environment variables (hosting dashboards, docker -e) always win over this file.
FREELLM_PORT=3001
# AGENT_PORT=8080          (on hosting platforms the PORT variable is used automatically)
# AGENT_HOST=127.0.0.1     (default: 127.0.0.1 locally, 0.0.0.0 on hosting platforms / Docker)
# PUBLIC_URL=https://my-agent.example.com
# live preview of generated HTML/apps: port (own origin on PREVIEW_PORT, scripts + localStorage work)
#   | inline (same port, sandboxed: scripts work, localStorage does not) | off.  Default: port locally, inline when hosted
PREVIEW_PORT=8081
# PREVIEW_MODE=
FREELLM_HOST=127.0.0.1
FREELLM_REPO=https://github.com/tashfeenahmed/freellmapi.git
FREELLM_BRANCH=main
UPDATE_CHECK_MINUTES=30
# plan | ask | acceptEdits | yolo   (yolo = agent auto-allows everything, default)
AGENT_MODE=yolo
# 0 = auto (adapts to the model and compacts by itself)
TOKEN_BUDGET=0
FREELLM_API_KEY=
FREELLM_ENCRYPTION_KEY=
AGENT_PASSWORD=
# Use ANY OpenAI-compatible API instead of FreeLLMAPI (skips clone/build; best for small hosts):
# LLM_BASE_URL=https://openrouter.ai/api/v1
# LLM_API_KEY=
# LLM_MODEL=
# FREELLM_ENABLED=0
# SELF_PING_MINUTES=0
`;
if (!fs.existsSync(ENV_FILE)) fs.writeFileSync(ENV_FILE, ENV_TEMPLATE, { mode: 0o600 });
let ENV = readEnv(ENV_FILE);
for (const k of Object.keys(ENV)) if (process.env[k]) ENV[k] = process.env[k];
for (const k of ['FREELLM_PORT', 'AGENT_PORT', 'PREVIEW_PORT', 'PREVIEW_MODE', 'FREELLM_HOST', 'AGENT_HOST', 'FREELLM_REPO', 'FREELLM_BRANCH',
  'UPDATE_CHECK_MINUTES', 'AGENT_MODE', 'TOKEN_BUDGET', 'FREELLM_API_KEY', 'FREELLM_ENCRYPTION_KEY', 'AGENT_PASSWORD', 'PUBLIC_URL',
  'LLM_BASE_URL', 'LLM_API_KEY', 'LLM_MODEL', 'FREELLM_ENABLED', 'SELF_PING_MINUTES', 'TRUST_PROXY_HOPS', 'ALLOWED_ORIGINS']) if (process.env[k]) ENV[k] = process.env[k];
const gen = {};
if (!ENV.FREELLM_ENCRYPTION_KEY) gen.FREELLM_ENCRYPTION_KEY = crypto.randomBytes(32).toString('hex');
if (!ENV.AGENT_PASSWORD) gen.AGENT_PASSWORD = crypto.randomBytes(9).toString('base64url');
if (Object.keys(gen).length) { upsertEnv(ENV_FILE, gen); Object.assign(ENV, gen); }

// precedence: real env var > (platform PORT when hosted) > .env file > default
const agentPort = Number(penv.AGENT_PORT || (HOSTED && penv.PORT) || ENV.AGENT_PORT || 8080);
const previewPort = Number(ENV.PREVIEW_PORT || agentPort + 1);
let freellmPort = Number(ENV.FREELLM_PORT || 3001);
if (freellmPort === agentPort || freellmPort === previewPort) freellmPort += 10; // a platform-assigned PORT may collide
const previewModeEnv = String(ENV.PREVIEW_MODE || '').toLowerCase();
const EXTERNAL_LLM = !!ENV.LLM_BASE_URL;                                    // any OpenAI-compatible API instead of FreeLLMAPI
const USE_FREELLM = !EXTERNAL_LLM && !/^(0|false|no|off)$/i.test(String(ENV.FREELLM_ENABLED || ''));
const detectedPublicUrl = (() => {
  const u = ENV.PUBLIC_URL || penv.RENDER_EXTERNAL_URL
    || (penv.RAILWAY_PUBLIC_DOMAIN && `https://${penv.RAILWAY_PUBLIC_DOMAIN}`)
    || (penv.FLY_APP_NAME && `https://${penv.FLY_APP_NAME}.fly.dev`)
    || (penv.REPLIT_DEV_DOMAIN && `https://${penv.REPLIT_DEV_DOMAIN}`)
    || (penv.KOYEB_PUBLIC_DOMAIN && `https://${penv.KOYEB_PUBLIC_DOMAIN}`)
    || (penv.SPACE_HOST && `https://${penv.SPACE_HOST}`)
    || (penv.HEROKU_APP_NAME && `https://${penv.HEROKU_APP_NAME}.herokuapp.com`)
    || (penv.CODESPACE_NAME && penv.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN && `https://${penv.CODESPACE_NAME}-${agentPort}.${penv.GITHUB_CODESPACES_PORT_FORWARDING_DOMAIN}`);
  return u ? String(u).replace(/\/+$/, '') : '';
})();
const CFG = {
  repo: ENV.FREELLM_REPO || 'https://github.com/tashfeenahmed/freellmapi.git',
  branch: ENV.FREELLM_BRANCH || 'main',
  dir: path.join(HOME, 'freellmapi'),
  port: freellmPort,
  host: ENV.FREELLM_HOST || '127.0.0.1',
  agentPort,
  agentHost: penv.AGENT_HOST || (HOSTED ? '0.0.0.0' : ENV.AGENT_HOST || '127.0.0.1'),
  previewPort,
  previewMode: ['port', 'inline', 'off'].includes(previewModeEnv) ? previewModeEnv : (HOSTED ? 'inline' : 'port'),
  publicUrl: detectedPublicUrl,
  updateMinutes: Number(ENV.UPDATE_CHECK_MINUTES ?? 30),
  encKey: ENV.FREELLM_ENCRYPTION_KEY,
  password: ENV.AGENT_PASSWORD,
  pwFromEnv: !!penv.AGENT_PASSWORD,
  trustProxyHops: Number(ENV.TRUST_PROXY_HOPS ?? (HOSTED ? 1 : 0)),     // how many reverse proxies sit in front (for client IPs)
  selfPingMinutes: Number(ENV.SELF_PING_MINUTES || 0),
};
const localUrl = (h, p) => `http://${h === '0.0.0.0' || h === '::' ? '127.0.0.1' : h}:${p}`;
const FREELLM_URL = localUrl(CFG.host, CFG.port);
const LLM_BASE = EXTERNAL_LLM ? String(ENV.LLM_BASE_URL).trim().replace(/\/+$/, '') : `${FREELLM_URL}/v1`;
const bootState = { phase: 'starting', message: 'Starting...' };            // exposed on /health and /api/settings
const setBoot = (phase, message) => { bootState.phase = phase; bootState.message = message; };
// Commands the agent runs must not inherit our secrets, nor our PORT (its dev servers would fight for it)
const SECRET_ENV = ['AGENT_PASSWORD', 'FREELLM_ENCRYPTION_KEY', 'FREELLM_API_KEY', 'LLM_API_KEY'];
const childEnv = (extra = {}) => { const e = { ...process.env }; for (const k of SECRET_ENV) delete e[k]; if (HOSTED) delete e.PORT; return Object.assign(e, extra); };
const WORKSPACE = path.join(HOME, 'workspace');
const CKPT_ROOT = path.join(DATA, 'checkpoints');
const MCP_FILE = path.join(HOME, 'mcp.json');
const HOOKS_FILE = path.join(HOME, 'hooks.json');
fs.mkdirSync(WORKSPACE, { recursive: true });
fs.mkdirSync(CKPT_ROOT, { recursive: true });

const K = { d: '\x1b[2m', c: '\x1b[36m', m: '\x1b[35m', y: '\x1b[33m', r: '\x1b[31m', g: '\x1b[32m', x: '\x1b[0m' };
const now = () => new Date().toLocaleTimeString();
const logTag = (tag, col) => (msg) => console.log(`${K.d}${now()}${K.x} ${col}[${tag}]${K.x} ${msg}`);
const log = logTag('launcher', K.c), warn = logTag('launcher', K.y), err = logTag('launcher', K.r), alog = logTag('agent', K.g);

// ============================================================================
// 1. AUTOMATION : clone / update / build / run / supervise FreeLLMAPI
// ============================================================================
const STATE_FILE = path.join(DATA, 'launcher-state.json');
const readState = () => { try { return JSON.parse(fs.readFileSync(STATE_FILE, 'utf8')); } catch { return {}; } };
const writeState = (s) => fs.writeFileSync(STATE_FILE, JSON.stringify({ ...readState(), ...s }, null, 2));

function run(cmd, args, { cwd = CFG.dir, quiet = false } = {}) {
  return new Promise((resolve, reject) => {
    if (!fs.existsSync(cwd)) cwd = HOME;
    const p = spawn(cmd, args, { cwd, env: childEnv(), shell: IS_WIN && cmd !== 'git', stdio: quiet ? ['ignore', 'pipe', 'pipe'] : 'inherit' });
    let out = '';
    p.stdout?.on('data', (d) => (out += d));
    p.stderr?.on('data', (d) => (out += d));
    p.on('error', reject);
    p.on('close', (code) => (code === 0 ? resolve(out.trim()) : reject(new Error(`${cmd} ${args.join(' ')} exited with ${code}\n${out.slice(-1500)}`))));
  });
}
const git = (args) => run('git', args, { quiet: true });
const npm = (args) => run(IS_WIN ? 'npm.cmd' : 'npm', args);
const headCommit = () => git(['rev-parse', 'HEAD']);

async function ensureClone() {
  if (fs.existsSync(path.join(CFG.dir, '.git'))) return;
  log(`Cloning ${CFG.repo} ...`);
  await run('git', ['clone', '--branch', CFG.branch, CFG.repo, CFG.dir], { cwd: HOME });
}
function syncFreeLLMEnv() {
  upsertEnv(path.join(CFG.dir, '.env'), { ENCRYPTION_KEY: CFG.encKey, PORT: String(CFG.port), HOST: CFG.host });
}
async function installAndBuild(force = false) {
  const head = await headCommit();
  const distOk = fs.existsSync(path.join(CFG.dir, 'server', 'dist', 'index.js'));
  if (!force && distOk && readState().builtCommit === head) { log(`FreeLLMAPI ${head.slice(0, 7)} already built.`); return; }
  log(`Installing FreeLLMAPI dependencies (${head.slice(0, 7)}). First time takes a few minutes...`);
  const lock = fs.existsSync(path.join(CFG.dir, 'package-lock.json'));
  try { await npm([lock ? 'ci' : 'install', '--no-audit', '--no-fund']); }
  catch { warn('npm ci failed, retrying with npm install...'); await npm(['install', '--no-audit', '--no-fund']); }
  log('Building FreeLLMAPI...');
  await npm(['run', 'build']);
  if (!fs.existsSync(path.join(CFG.dir, 'server', 'dist', 'index.js'))) throw new Error('Build done but server/dist/index.js is missing (repo layout changed?).');
  writeState({ builtCommit: head, builtAt: new Date().toISOString() });
}
function backupFreeLLMData() {
  const root = path.join(DATA, 'freellmapi-backups');
  const dest = path.join(root, new Date().toISOString().replace(/[:.]/g, '-'));
  fs.mkdirSync(dest, { recursive: true });
  const db = path.join(CFG.dir, 'server', 'data');
  if (fs.existsSync(db)) fs.cpSync(db, path.join(dest, 'server-data'), { recursive: true });
  const all = fs.readdirSync(root).sort();
  for (const old of all.slice(0, Math.max(0, all.length - 5))) fs.rmSync(path.join(root, old), { recursive: true, force: true });
  return dest;
}
async function reclone(backupDir) {
  warn('Normal update failed, doing a clean re-clone (data is restored after)...');
  const old = `${CFG.dir}.old`;
  fs.rmSync(old, { recursive: true, force: true });
  fs.renameSync(CFG.dir, old);
  try {
    await run('git', ['clone', '--branch', CFG.branch, CFG.repo, CFG.dir], { cwd: HOME });
    const saved = path.join(backupDir, 'server-data');
    if (fs.existsSync(saved)) fs.cpSync(saved, path.join(CFG.dir, 'server', 'data'), { recursive: true });
    fs.rmSync(old, { recursive: true, force: true });
  } catch (e) { fs.rmSync(CFG.dir, { recursive: true, force: true }); fs.renameSync(old, CFG.dir); throw e; }
}

let updating = false, shuttingDown = false;
async function checkForUpdate({ restart = true } = {}) {
  if (updating) return;
  updating = true;
  try {
    const local = await headCommit();
    await git(['fetch', '--quiet', 'origin', CFG.branch]);
    const remote = await git(['rev-parse', `origin/${CFG.branch}`]);
    if (local === remote) { log(`FreeLLMAPI up to date (${local.slice(0, 7)}).`); return; }
    log(`${K.g}Update found${K.x} ${local.slice(0, 7)} -> ${remote.slice(0, 7)}. Redeploying...`);
    if (restart) await freellm.stop();
    const backup = backupFreeLLMData();
    try { await git(['reset', '--hard', `origin/${CFG.branch}`]); }
    catch { await reclone(backup); }
    syncFreeLLMEnv();
    try { await installAndBuild(true); }
    catch (e) { err(`New version failed to build, rolling back to ${local.slice(0, 7)}.\n${e.message}`); await git(['reset', '--hard', local]); await installAndBuild(true); }
  } catch (e) {
    err(`Update check failed: ${e.message.split('\n')[0]}`);
  } finally {
    updating = false;
    if (restart && !freellm.running && !shuttingDown) await freellm.start().catch((e) => err(e.message));
  }
}

async function waitFor(url, name, ms = 180000) {
  const t0 = Date.now();
  while (Date.now() - t0 < ms) {
    try { const r = await fetch(url, { signal: AbortSignal.timeout(3000) }); if (r.status < 500) return log(`${name} is up: ${url}`); } catch { /* not yet */ }
    await new Promise((r) => setTimeout(r, 1000));
  }
  throw new Error(`${name} did not answer at ${url} within ${ms / 1000}s`);
}

const freellm = (() => {
  let child = null, stopping = false, restarts = 0;
  const out = logTag('freellmapi', K.m);
  const api = {
    get running() { return !!child; },
    async start() {
      stopping = false;
      child = spawn(process.execPath, ['server/dist/index.js'], {
        cwd: CFG.dir, stdio: ['ignore', 'pipe', 'pipe'],
        env: childEnv({ NODE_ENV: 'production', ENCRYPTION_KEY: CFG.encKey, PORT: String(CFG.port), HOST: CFG.host }),
      });
      const pipe = (s) => s.on('data', (d) => d.toString().split(/\r?\n/).filter(Boolean).forEach(out));
      pipe(child.stdout); pipe(child.stderr);
      child.on('exit', (code) => {
        child = null;
        if (stopping || shuttingDown) return;
        const delay = Math.min(30000, 1000 * 2 ** Math.min(++restarts, 5));
        err(`FreeLLMAPI exited (code ${code}). Restarting in ${delay / 1000}s...`);
        setTimeout(() => !shuttingDown && !updating && !child && api.start().catch((e) => err(e.message)), delay);
      });
      await waitFor(`${FREELLM_URL}/`, 'FreeLLMAPI');
      restarts = 0;
    },
    stop() {
      return new Promise((resolve) => {
        if (!child) return resolve();
        stopping = true;
        const c = child, t = setTimeout(() => c.kill('SIGKILL'), 10000);
        c.once('exit', () => { clearTimeout(t); resolve(); });
        c.kill();
      });
    },
  };
  return api;
})();

// ============================================================================
// 2. CORE UTILS
// ============================================================================
const readJson = (f, d) => { try { return JSON.parse(fs.readFileSync(f, 'utf8')); } catch { return d; } };
const writeJson = (f, v) => { fs.mkdirSync(path.dirname(f), { recursive: true }); fs.writeFileSync(`${f}.tmp`, JSON.stringify(v, null, 2)); fs.renameSync(`${f}.tmp`, f); };
const newId = (n = 8) => crypto.randomBytes(n).toString('hex');
const CONV_DIR = path.join(DATA, 'conversations');
const SETTINGS_FILE = path.join(DATA, 'settings.json');
const MEMORY_FILE = path.join(DATA, 'memory.json');
fs.mkdirSync(CONV_DIR, { recursive: true });

const clip = (s, n = 12000) => (String(s).length > n ? `${String(s).slice(0, n)}\n...[truncated ${String(s).length - n} chars]` : String(s));
const estTokens = (s) => Math.ceil(String(s).length / 3.6);
const anySignal = (signals) => {
  const list = signals.filter(Boolean);
  if (list.length === 1) return list[0];
  if (AbortSignal.any) return AbortSignal.any(list);
  const ac = new AbortController();
  for (const s of list) s.addEventListener('abort', () => ac.abort(s.reason), { once: true });
  return ac.signal;
};

function wsPath(p = '.') {
  const full = path.resolve(WORKSPACE, String(p || '.').replace(/^[/\\]+/, ''));
  if (full !== WORKSPACE && !full.startsWith(WORKSPACE + path.sep)) throw new Error(`Path escapes the workspace: ${p}`);
  return full;
}
const relPath = (full) => path.relative(WORKSPACE, full).split(path.sep).join('/');
const fileUrl = (full) => `/files/${relPath(full).split('/').map(encodeURIComponent).join('/')}`;

const IGNORE_DIRS = new Set(['node_modules', '.git', 'dist', 'build', '.next', 'target', '__pycache__', '.venv', 'venv', '.cache', 'coverage', '.power-agent', 'vendor', '.idea', '.vscode']);
const TEXT_EXT = new Set(['.js', '.mjs', '.cjs', '.ts', '.tsx', '.jsx', '.json', '.md', '.txt', '.yml', '.yaml', '.toml', '.ini', '.cfg', '.env', '.css', '.scss', '.html', '.htm', '.xml', '.svg', '.py', '.rb', '.go', '.rs', '.java', '.kt', '.c', '.h', '.cpp', '.hpp', '.cs', '.php', '.sh', '.bash', '.zsh', '.ps1', '.sql', '.graphql', '.vue', '.svelte', '.lua', '.r', '.swift', '.dart', '.ex', '.exs', '.erl', '.clj', '.zig', '.nim', '.tf', '.dockerfile', '.gitignore', '.editorconfig']);
const IMG_EXT = new Set(['.png', '.jpg', '.jpeg', '.gif', '.webp', '.avif', '.bmp', '.ico', '.svg']);
function isBinary(buf) { return buf.subarray(0, 8000).includes(0); }
function looksTexty(file) {
  const base = path.basename(file).toLowerCase();
  if (base === 'dockerfile' || base === 'makefile' || base.startsWith('.env')) return true;
  return TEXT_EXT.has(path.extname(base));
}

function* walk(dir, { depth = 12, maxFiles = 20000, followHidden = false } = {}) {
  let count = 0;
  const stack = [[dir, 0]];
  while (stack.length) {
    const [d, lvl] = stack.pop();
    if (lvl > depth) continue;
    let ents;
    try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch { continue; }
    for (const e of ents) {
      if (count++ > maxFiles) return;
      if (!followHidden && e.name.startsWith('.') && e.name !== '.env' && e.name !== '.gitignore') continue;
      const full = path.join(d, e.name);
      if (e.isDirectory()) { if (!IGNORE_DIRS.has(e.name)) stack.push([full, lvl + 1]); }
      else if (e.isFile()) yield full;
    }
  }
}

let ckptSeq = 0;
function checkpoint(convId, label, files) {
  const id = `${Date.now().toString(36)}-${ckptSeq++}`;
  const dir = path.join(CKPT_ROOT, convId, id);
  const entries = [];
  for (const f of files) {
    if (!f.startsWith(WORKSPACE)) continue;
    const rel = relPath(f);
    if (fs.existsSync(f) && fs.statSync(f).isFile()) {
      const dest = path.join(dir, 'files', rel);
      fs.mkdirSync(path.dirname(dest), { recursive: true });
      fs.copyFileSync(f, dest);
      entries.push({ rel, existed: true });
    } else entries.push({ rel, existed: false });
  }
  writeJson(path.join(dir, 'manifest.json'), { id, label, at: new Date().toISOString(), entries });
  return id;
}
function restoreCheckpoint(convId, id) {
  const dir = path.join(CKPT_ROOT, convId, id);
  const man = readJson(path.join(dir, 'manifest.json'), null);
  if (!man) throw new Error('Checkpoint not found.');
  for (const e of man.entries) {
    const target = wsPath(e.rel);
    if (e.existed) { fs.mkdirSync(path.dirname(target), { recursive: true }); fs.copyFileSync(path.join(dir, 'files', e.rel), target); }
    else fs.rmSync(target, { force: true });
  }
  return man.entries.map((e) => e.rel);
}
function listCheckpoints(convId) {
  try {
    return fs.readdirSync(path.join(CKPT_ROOT, convId)).sort().reverse()
      .map((id) => readJson(path.join(CKPT_ROOT, convId, id, 'manifest.json'), null)).filter(Boolean);
  } catch { return []; }
}

const getHooks = () => readJson(HOOKS_FILE, { afterEdit: [] });
function runHooks(kind, file) {
  const hooks = getHooks()[kind] || [];
  const out = [];
  for (const tpl of hooks) {
    const cmd = tpl.replaceAll('{file}', relPath(file));
    try {
      const r = spawnSync(cmd, { shell: true, cwd: WORKSPACE, encoding: 'utf8', timeout: 60000, env: childEnv() });
      const text = `${r.stdout || ''}${r.stderr || ''}`.trim();
      out.push(r.status === 0 ? text : `hook failed (${r.status}): ${text.slice(0, 300)}`);
    } catch (e) { out.push(`hook failed: ${e.message}`); }
  }
  return out.filter(Boolean).join('\n');
}

// ============================================================================
// 3. SETTINGS
// ============================================================================
const DEFAULTS = {
  apiKey: '', baseUrl: '', model: ENV.LLM_MODEL || 'auto', imageModel: '', allowShell: true, maxSteps: 0, // 0 = unlimited
  temperature: 0.3, persona: '', mode: ENV.AGENT_MODE || 'yolo',
  tokenBudget: Number(ENV.TOKEN_BUDGET || 0), maxParallel: 8, // 0 = auto
  autoApprove: [], fallbackModels: [], disabledTools: [],
};
const AUTO_BUDGET = 200000;                 // starting point when tokenBudget = 0 (auto)
const learnedBudget = new Map();            // model -> real limit learned from provider errors
const effBudget = (s) => (s.tokenBudget > 0 ? s.tokenBudget : AUTO_BUDGET);
function getSettings() {
  const s = { ...DEFAULTS, ...readJson(SETTINGS_FILE, {}) };
  const envKey = EXTERNAL_LLM ? (ENV.LLM_API_KEY || ENV.FREELLM_API_KEY) : (ENV.FREELLM_API_KEY || ENV.LLM_API_KEY);
  if (!s.apiKey && envKey) s.apiKey = envKey;
  if (s.model === 'auto' && ENV.LLM_MODEL && (EXTERNAL_LLM || s.baseUrl)) s.model = ENV.LLM_MODEL;
  return s;
}
function saveSettings(patch) {
  const next = readJson(SETTINGS_FILE, {});
  for (const k of Object.keys(DEFAULTS)) if (k in patch) next[k] = patch[k];
  next.maxSteps = Math.max(0, Math.floor(Number(next.maxSteps ?? 0) || 0)); // 0 = unlimited
  next.maxParallel = Math.max(1, Math.min(32, Number(next.maxParallel ?? 8) || 8));
  { const tb = Math.floor(Number(next.tokenBudget ?? 0) || 0); next.tokenBudget = tb > 0 ? Math.max(8000, Math.min(2000000, tb)) : 0; } // 0 = auto
  next.temperature = Math.max(0, Math.min(2, Number(next.temperature ?? 0.3) || 0));
  next.allowShell = next.allowShell !== false;
  next.baseUrl = String(next.baseUrl || '').trim().replace(/\/+$/, '');
  if (next.baseUrl && !/^https?:\/\/\S+$/i.test(next.baseUrl)) next.baseUrl = '';
  if (!['plan', 'ask', 'acceptEdits', 'yolo'].includes(next.mode)) next.mode = 'yolo';
  if (!Array.isArray(next.autoApprove)) next.autoApprove = [];
  if (!Array.isArray(next.fallbackModels)) next.fallbackModels = [];
  writeJson(SETTINGS_FILE, next);
  return getSettings();
}
// v4 one-time migration: old installs had mode=ask, 40 steps, 60k budget. Switch to auto-allow + unlimited.
(function migrateSettings() {
  const raw = readJson(SETTINGS_FILE, {});
  if (raw._v4) return;
  writeJson(SETTINGS_FILE, { ...raw, _v4: true, mode: 'yolo', maxSteps: 0, tokenBudget: 0, maxParallel: 8, autoApprove: [] });
})();

// ============================================================================
// 4. CONVERSATIONS
// ============================================================================
const convFile = (id) => path.join(CONV_DIR, `${String(id).replace(/[^a-f0-9]/g, '')}.json`);
const listConversations = () => fs.readdirSync(CONV_DIR).filter((f) => f.endsWith('.json'))
  .map((f) => readJson(path.join(CONV_DIR, f), null)).filter(Boolean)
  .map(({ id, title, updatedAt, todos }) => ({ id, title, updatedAt, open: (todos || []).filter((t) => t.status !== 'done').length }))
  .sort((a, b) => b.updatedAt.localeCompare(a.updatedAt));
const getConversation = (id) => readJson(convFile(id), null);
function createConversation(title = 'New chat') {
  const t = new Date().toISOString();
  const c = { id: newId(), title, createdAt: t, updatedAt: t, messages: [], todos: [], summary: '' };
  writeJson(convFile(c.id), c);
  return c;
}
const saveConversation = (c) => { c.updatedAt = new Date().toISOString(); writeJson(convFile(c.id), c); };

// ============================================================================
// 5. MEMORY
// ============================================================================
const STOP = new Set('a an the and or but of to in on at for with is are was were be been it this that i you me my your we our they he she them his her its as by from about what which who how do does did not no yes can could would should will just so if then than there here ang ng sa na at mga ko mo ka ako ikaw siya ito yan yun po'.split(' '));
const tokens = (s) => String(s).toLowerCase().normalize('NFKD').replace(/[^\p{L}\p{N}\s]/gu, ' ').split(/\s+/).filter((w) => w.length > 1 && !STOP.has(w));
const listMemories = () => readJson(MEMORY_FILE, []);
function addMemory(text, tags = []) {
  const mem = listMemories(), clean = String(text || '').trim().slice(0, 1000);
  if (!clean) throw new Error('Empty memory');
  const dup = mem.find((m) => m.text.toLowerCase() === clean.toLowerCase());
  if (dup) return dup;
  const m = { id: newId(), text: clean, tags, createdAt: new Date().toISOString() };
  writeJson(MEMORY_FILE, [...mem, m]);
  return m;
}
function deleteMemory(id) { const mem = listMemories(), next = mem.filter((m) => m.id !== id); writeJson(MEMORY_FILE, next); return next.length !== mem.length; }
function searchMemories(q, limit = 8) {
  const mem = listMemories();
  if (!mem.length) return [];
  const qs = new Set(tokens(q)), df = {};
  const docs = mem.map((m) => { const t = tokens(`${m.text} ${(m.tags || []).join(' ')}`); new Set(t).forEach((w) => (df[w] = (df[w] || 0) + 1)); return t; });
  return mem.map((m, i) => {
    let s = 0;
    for (const w of docs[i]) if (qs.has(w)) s += Math.log(1 + mem.length / df[w]);
    return { ...m, score: s / Math.sqrt(docs[i].length + 1) };
  }).filter((m) => m.score > 0).sort((a, b) => b.score - a.score).slice(0, limit);
}

// ============================================================================
// 6. PROJECT MEMORY + REPO MAP
// ============================================================================
const PROJECT_FILES = ['POWERAGENT.md', 'AGENTS.md', 'CLAUDE.md', '.poweragent.md'];
function projectMemory() {
  const parts = [];
  for (const f of PROJECT_FILES) {
    const p = path.join(WORKSPACE, f);
    if (fs.existsSync(p)) parts.push(`### ${f}\n${clip(fs.readFileSync(p, 'utf8'), 6000)}`);
  }
  return parts.join('\n\n');
}

const DECL = [
  [/^\s*(?:export\s+)?(?:default\s+)?(?:async\s+)?function\s+([A-Za-z_$][\w$]*)/, 'fn'],
  [/^\s*(?:export\s+)?(?:const|let|var)\s+([A-Za-z_$][\w$]*)\s*=\s*(?:async\s*)?(?:\(|function)/, 'fn'],
  [/^\s*(?:export\s+)?class\s+([A-Za-z_$][\w$]*)/, 'class'],
  [/^\s*(?:export\s+)?(?:interface|type|enum)\s+([A-Za-z_$][\w$]*)/, 'type'],
  [/^\s*def\s+([A-Za-z_]\w*)/, 'def'],
  [/^\s*class\s+([A-Za-z_]\w*)\s*[:(]/, 'class'],
  [/^\s*func\s+(?:\([^)]*\)\s*)?([A-Za-z_]\w*)/, 'fn'],
  [/^\s*(?:pub\s+)?fn\s+([A-Za-z_]\w*)/, 'fn'],
  [/^\s*(?:public|private|protected|static|\s)+[\w<>[\]]+\s+([A-Za-z_]\w*)\s*\(/, 'method'],
  [/^\s*#\s*([A-Z][\w ]+)/, 'section'],
];
const MAP_EXT = new Set(['.js', '.mjs', '.cjs', '.ts', '.tsx', '.jsx', '.py', '.go', '.rs', '.java', '.kt', '.rb', '.php', '.cs', '.c', '.h', '.cpp', '.hpp', '.swift', '.lua', '.sh', '.vue', '.svelte', '.sql', '.md']);
let mapCache = { at: 0, text: '' };
function repoMap(maxChars = 9000) {
  if (Date.now() - mapCache.at < 20000) return mapCache.text;
  const lines = [];
  let budget = maxChars;
  for (const file of walk(WORKSPACE, { depth: 8, maxFiles: 4000 })) {
    if (budget <= 0) break;
    if (!MAP_EXT.has(path.extname(file).toLowerCase())) continue;
    let src;
    try { if (fs.statSync(file).size > 400000) continue; src = fs.readFileSync(file, 'utf8'); } catch { continue; }
    const rel = relPath(file), hits = [];
    src.split('\n').forEach((line, i) => {
      if (line.length > 200) return;
      for (const [re, kind] of DECL) {
        const m = line.match(re);
        if (m) { hits.push(`  ${i + 1}: ${kind} ${m[1]}`); break; }
      }
    });
    if (!hits.length) continue;
    const block = `${rel}\n${hits.slice(0, 40).join('\n')}`;
    if (block.length > budget) break;
    budget -= block.length;
    lines.push(block);
  }
  const text = lines.join('\n');
  mapCache = { at: Date.now(), text };
  return text;
}

// ============================================================================
// 7. SEARCH
// ============================================================================
function globToRe(g) {
  let re = '';
  for (let i = 0; i < g.length; i++) {
    const ch = g[i];
    if (ch === '*') { if (g[i + 1] === '*') { re += '.*'; i++; if (g[i + 1] === '/') i++; } else re += '[^/]*'; }
    else if (ch === '?') re += '[^/]';
    else if ('\\^$.|+()[]{}'.includes(ch)) re += `\\${ch}`;
    else re += ch;
  }
  return new RegExp(`^${re}$`);
}
function findFiles(pattern, { limit = 300, root = '.' } = {}) {
  const base = wsPath(root), re = globToRe(pattern.replace(/^\.\//, ''));
  const out = [];
  for (const f of walk(base, { maxFiles: 60000 })) {
    if (out.length >= limit) break;
    const rel = relPath(f);
    if (re.test(rel) || re.test(path.basename(f))) out.push(f);
  }
  return out;
}
function grepTree({ pattern, glob, root = '.', ignoreCase = false, literal = false, limit = 200, contextLines = 0 }) {
  const base = wsPath(root);
  const src = literal ? pattern.replace(/[.*+?^${}()|[\]\\]/g, '\\$&') : pattern;
  let re;
  try { re = new RegExp(src, ignoreCase ? 'gi' : 'g'); } catch (e) { throw new Error(`Bad regex: ${e.message}`); }
  const globRe = glob ? globToRe(glob) : null;
  const hits = [];
  for (const f of walk(base, { maxFiles: 60000 })) {
    if (hits.length >= limit) break;
    if (globRe && !globRe.test(relPath(f))) continue;
    if (!looksTexty(f)) continue;
    let buf;
    try { if (fs.statSync(f).size > 2e6) continue; buf = fs.readFileSync(f); } catch { continue; }
    if (isBinary(buf)) continue;
    const lines = buf.toString('utf8').split('\n');
    for (let i = 0; i < lines.length; i++) {
      re.lastIndex = 0;
      if (!re.test(lines[i])) continue;
      const from = Math.max(0, i - contextLines), to = Math.min(lines.length, i + contextLines + 1);
      hits.push(`${relPath(f)}:${i + 1}: ${lines.slice(from, to).map((l) => l.slice(0, 300)).join('\n    ')}`);
      if (hits.length >= limit) break;
    }
  }
  return hits;
}
function symbolFind(name, limit = 60) {
  const out = [];
  for (const f of walk(WORKSPACE, { maxFiles: 20000 })) {
    if (!MAP_EXT.has(path.extname(f).toLowerCase())) continue;
    let src;
    try { if (fs.statSync(f).size > 400000) continue; src = fs.readFileSync(f, 'utf8'); } catch { continue; }
    src.split('\n').forEach((line, i) => {
      if (line.length > 200 || !line.includes(name)) return;
      for (const [re, kind] of DECL) {
        const m = line.match(re);
        if (m && m[1] === name) { out.push(`${relPath(f)}:${i + 1}: ${kind} ${line.trim().slice(0, 160)}`); break; }
      }
    });
    if (out.length >= limit) break;
  }
  return out;
}

// ============================================================================
// 8. PATCH ENGINE
// ============================================================================
const normLines = (s) => s.replace(/\r\n/g, '\n').replace(/\t/g, '  ').split('\n').map((l) => l.replace(/[ \t]+$/, ''));
const squeeze = (s) => s.replace(/[ \t]+/g, ' ').trim();

function locate(content, search) {
  const hay = normLines(content), needle = normLines(search);
  while (needle.length && needle[0].trim() === '') needle.shift();
  while (needle.length && needle.at(-1).trim() === '') needle.pop();
  if (!needle.length) return null;
  const matches = [];
  outer: for (let i = 0; i + needle.length <= hay.length; i++) {
    for (let j = 0; j < needle.length; j++) {
      if (hay[i + j].replace(/^[ \t]+/, '') !== needle[j].replace(/^[ \t]+/, '')) continue outer;
    }
    matches.push(i);
  }
  if (matches.length) return { start: matches[0], end: matches[0] + needle.length, count: matches.length, exact: true };
  const sq = needle.map(squeeze).join('\n');
  const fuzzy = [];
  for (let i = 0; i + needle.length <= hay.length; i++) {
    if (hay.slice(i, i + needle.length).map(squeeze).join('\n') === sq) fuzzy.push(i);
  }
  if (fuzzy.length) return { start: fuzzy[0], end: fuzzy[0] + needle.length, count: fuzzy.length, exact: false };
  return null;
}

function parsePatch(text) {
  const lines = String(text).replace(/\r\n/g, '\n').split('\n');
  const ops = [];
  let file = null, i = 0;
  const header = (l) => {
    const m = l.match(/^\*\*\* (?:Update|Add|Create) File:\s*(.+)$/) || l.match(/^\+\+\+ b\/(.+)$/);
    if (m) { file = m[1].trim(); return true; }
    return false;
  };
  while (i < lines.length) {
    if (header(lines[i])) { i++; continue; }
    if (/^<{5,}\s*SEARCH|^<{7}/.test(lines[i])) {
      const search = [], replace = [];
      i++;
      while (i < lines.length && !/^={5,}|^={7}/.test(lines[i])) search.push(lines[i++]);
      i++;
      while (i < lines.length && !/^>{5,}\s*REPLACE|^>{7}/.test(lines[i])) replace.push(lines[i++]);
      i++;
      if (!file) throw new Error('Patch block without a file header. Put the path on its own line before <<<<<<< SEARCH.');
      ops.push({ file, search: search.join('\n'), replace: replace.join('\n') });
      continue;
    }
    i++;
  }
  return ops;
}

function applyOpsToFile(file, ops) {
  let content = fs.existsSync(file) ? fs.readFileSync(file, 'utf8') : null;
  if (content == null && !ops.every((o) => !o.search.trim())) throw new Error(`${relPath(file)}: file does not exist (use create_file).`);
  const eol = content?.includes('\r\n') ? '\r\n' : '\n';
  let work = content == null ? '' : content.replace(/\r\n/g, '\n');
  const applied = [];
  for (const op of ops) {
    if (!op.search.trim()) {
      if (op.replace.trim()) { work = op.replace + (work ? `\n${work}` : ''); applied.push('created'); }
      continue;
    }
    const hit = locate(work, op.search);
    if (!hit) throw new Error(`${relPath(file)}: could not locate:\n${clip(op.search, 400)}`);
    if (hit.count > 1) applied.push(`ambiguous(${hit.count})`);
    const before = work.split('\n').slice(0, hit.start);
    const after = work.split('\n').slice(hit.end);
    work = [...before, ...op.replace.replace(/\r\n/g, '\n').split('\n'), ...after].join('\n');
    applied.push(hit.exact ? 'ok' : 'fuzzy');
  }
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, eol === '\r\n' ? work.replace(/\n/g, '\r\n') : work);
  return applied.join(',');
}

function diffStat(before, after) {
  const a = new Set(normLines(before)), b = new Set(normLines(after));
  let add = 0, del = 0;
  for (const l of b) if (!a.has(l)) add++;
  for (const l of a) if (!b.has(l)) del++;
  return { add, del };
}

function previewDiff(before, after, file) {
  const A = normLines(before), B = normLines(after), out = [];
  let i = 0, j = 0;
  while (i < A.length || j < B.length) {
    if (A[i] === B[j]) { i++; j++; continue; }
    let k = 1;
    while (k < 40 && (A[i + k] !== B[j] || A[i] !== B[j + k])) k++;
    if (j < B.length && (i >= A.length || A[i + k] === B[j] || B[j + k] !== A[i])) { out.push(`+ ${B[j]}`); j++; }
    else if (i < A.length) { out.push(`- ${A[i]}`); i++; }
    if (out.length > 60) break;
  }
  return `--- ${relPath(file)}\n${out.join('\n')}`;
}

// ============================================================================
// 9. SHELL RUNNER
// ============================================================================
const jobs = new Map();
const DENY = [
  /^\s*(sudo\s+)?rm\s+-[a-z]*r[a-z]*\s+(--no-preserve-root\s+)?(\/|~|\$HOME)\*?\s*$/i,
  /^\s*(sudo\s+)?(shutdown|reboot|halt|poweroff)\b/i,
  /\bmkfs(\.\w+)?\s+\/dev\//i,
  /\bdd\s+[^|;&]*of=\/dev\/(sd|nvme|hd|disk)/i,
  /:\(\)\s*\{\s*:\|:&\s*\};:/,
  /^\s*format\s+[a-z]:/i,
]; // only machine-destroying commands are blocked; everything else is the agent's call
function shellRun(command, { cwd = WORKSPACE, timeout = 120000, signal, background = false } = {}) {
  if (background) {
    const id = newId();
    const child = spawn(command, { cwd, shell: true, windowsHide: true, env: childEnv() });
    const job = { id, command, out: '', child, startedAt: Date.now(), done: false, code: null };
    const push = (d) => { job.out = clip(job.out + d.toString(), 60000); };
    child.stdout.on('data', push); child.stderr.on('data', push);
    child.on('exit', (c) => { job.done = true; job.code = c; });
    jobs.set(id, job);
    return `Started background job ${id}: ${command}`;
  }
  return new Promise((resolve) => {
    const child = spawn(command, { cwd, shell: true, windowsHide: true, env: childEnv() });
    let out = '', done = false;
    const finish = (note) => {
      if (done) return; done = true;
      resolve(clip(`${note}\n--- output ---\n${out || '(no output)'}`));
    };
    const push = (d) => { if (out.length < 60000) out += d.toString(); };
    child.stdout.on('data', push); child.stderr.on('data', push);
    const timer = setTimeout(() => {
      try { IS_WIN ? exec(`taskkill /pid ${child.pid} /T /F`) : child.kill('SIGKILL'); } catch { /* ignore */ }
      finish('exit: TIMEOUT');
    }, timeout);
    signal?.addEventListener('abort', () => { try { child.kill('SIGKILL'); } catch { /* ignore */ } finish('exit: ABORTED'); }, { once: true });
    child.on('error', (e) => { clearTimeout(timer); finish(`exit: spawn error ${e.message}`); });
    child.on('close', (code) => { clearTimeout(timer); finish(`exit: ${code}`); });
  });
}

// ---- machine awareness: what is installed, how to install more ----
const exists = (p) => { try { return !!p && fs.existsSync(p); } catch { return false; } };
function which(name) {
  if (!name || !/^[A-Za-z0-9._+/-]+$/.test(name)) return '';
  try {
    const r = IS_WIN ? spawnSync('where', [name], { encoding: 'utf8', timeout: 5000 }) : spawnSync('sh', ['-c', `command -v ${name}`], { encoding: 'utf8', timeout: 5000 });
    return r.status === 0 ? String(r.stdout).split(/\r?\n/)[0].trim() : '';
  } catch { return ''; }
}
let chromeCache = null, probeCache = null, probeAt = 0, previewUp = false;
function browserCacheHits() {
  const home = process.env.HOME || process.env.USERPROFILE || '';
  const roots = [path.join(home, '.cache', 'ms-playwright'), process.env.PLAYWRIGHT_BROWSERS_PATH, path.join(HOME, 'browsers'), path.join(home, '.cache', 'puppeteer'), path.join(home, '.remotion')].filter(Boolean);
  const names = new Set(['chrome', 'chrome.exe', 'chrome-headless-shell', 'headless_shell', 'chromium', 'Chromium']);
  const hits = [];
  const walk = (d, depth) => {
    if (depth > 6) return;
    let ents; try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch { return; }
    for (const e of ents) { const full = path.join(d, e.name); if (e.isFile() && names.has(e.name)) hits.push(full); else if (e.isDirectory()) walk(full, depth + 1); }
  };
  roots.forEach((r) => walk(r, 0));
  return hits;
}
function findChrome() {
  if (chromeCache && exists(chromeCache)) return chromeCache;
  const fixed = [
    process.env.CHROME_PATH, process.env.PUPPETEER_EXECUTABLE_PATH, process.env.CHROME_BIN,
    IS_WIN ? `${process.env['PROGRAMFILES']}\\Google\\Chrome\\Application\\chrome.exe` : null,
    IS_WIN ? `${process.env['PROGRAMFILES(X86)']}\\Google\\Chrome\\Application\\chrome.exe` : null,
    IS_WIN ? `${process.env.LOCALAPPDATA}\\Google\\Chrome\\Application\\chrome.exe` : null,
    IS_WIN ? `${process.env['PROGRAMFILES']}\\Microsoft\\Edge\\Application\\msedge.exe` : null,
    '/usr/bin/google-chrome', '/usr/bin/google-chrome-stable', '/usr/bin/chromium', '/usr/bin/chromium-browser', '/snap/bin/chromium',
    '/usr/local/bin/chromium', '/data/data/com.termux/files/usr/bin/chromium', '/data/data/com.termux/files/usr/bin/chromium-browser',
    '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome', '/Applications/Chromium.app/Contents/MacOS/Chromium',
  ].filter(Boolean);
  let hit = fixed.find(exists);
  if (!hit) for (const n of ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser', 'chrome', 'chrome-headless-shell', 'microsoft-edge', 'msedge']) { hit = which(n); if (hit) break; }
  if (!hit) hit = browserCacheHits()[0];
  chromeCache = hit || null;
  return chromeCache;
}
const PKG_MANAGERS = ['pkg', 'apt-get', 'brew', 'dnf', 'yum', 'pacman', 'apk', 'zypper', 'winget', 'choco', 'scoop'];
const pickInstaller = (p) => PKG_MANAGERS.find((m) => p.have[m]) || null;
function sysProbe(fresh = false) {
  if (!fresh && probeCache && Date.now() - probeAt < 300000) return { ...probeCache, chrome: findChrome() };
  const have = {};
  for (const n of ['node', 'npm', 'npx', 'git', 'python3', 'python', 'pip', 'pip3', 'ffmpeg', 'ffprobe', 'curl', 'wget', 'unzip', 'zip', 'tar', 'docker', 'sudo', 'make', 'gcc', 'go', 'cargo', 'java', 'bun', 'deno', ...PKG_MANAGERS]) { const w = which(n); if (w) have[n] = w; }
  let distro = '';
  try { distro = (fs.readFileSync('/etc/os-release', 'utf8').match(/^PRETTY_NAME="?([^"\n]+)/m) || [])[1] || ''; } catch { /* not linux */ }
  probeCache = {
    platform: process.platform, arch: process.arch, distro, node: process.version,
    isTermux: IS_TERMUX, hostPlatform: PLATFORM || null, hosted: HOSTED,
    root: typeof process.getuid === 'function' ? process.getuid() === 0 : false,
    cpus: os.cpus().length, memGB: Math.round((os.totalmem() / 1e9) * 10) / 10, freeDiskNote: 'check with df -h',
    installer: null, have, workspace: WORKSPACE,
  };
  probeCache.installer = pickInstaller(probeCache);
  probeAt = Date.now();
  return { ...probeCache, chrome: findChrome() };
}
const envLine = () => {
  const p = sysProbe();
  return `${p.platform}/${p.arch}${p.distro ? ` (${p.distro})` : ''}${p.isTermux ? ' [Termux/Android]' : ''}${p.hostPlatform && !p.isTermux ? ` [host: ${p.hostPlatform}]` : ''}; ${p.root ? 'root' : 'non-root'}${p.have.sudo && !p.root ? '+sudo' : ''}; installer=${p.installer || 'none'}; installed: ${Object.keys(p.have).filter((k) => !PKG_MANAGERS.includes(k) || k === p.installer).join(', ')}; chrome=${p.chrome || 'MISSING (install_package ["chromium"])'}; ffmpeg=${p.have.ffmpeg ? 'yes' : 'MISSING (install_package ["ffmpeg"])'}; cpus=${p.cpus}; ram=${p.memGB}GB`;
};
const mapPkg = (m, n) => {
  if (/^python3?$/.test(n)) return m === 'pkg' || m === 'brew' ? 'python' : 'python3';
  if (/^pip3?$/.test(n)) return m === 'apt-get' ? 'python3-pip' : m === 'pkg' ? 'python-pip' : n;
  if (n === 'node' || n === 'nodejs') return m === 'brew' ? 'node' : 'nodejs';
  return n;
};
function installCmd(m, names, sudo) {
  const l = names.join(' ');
  switch (m) {
    case 'pkg': return `pkg install -y ${l}`;
    case 'apt-get': return `${sudo}apt-get update -y; ${sudo}env DEBIAN_FRONTEND=noninteractive apt-get install -y ${l}`;
    case 'brew': return `brew install ${l}`;
    case 'dnf': return `${sudo}dnf install -y ${l}`;
    case 'yum': return `${sudo}yum install -y ${l}`;
    case 'pacman': return `${sudo}pacman -S --noconfirm --needed ${l}`;
    case 'apk': return `${sudo}apk add --no-cache ${l}`;
    case 'zypper': return `${sudo}zypper --non-interactive install ${l}`;
    case 'winget': return names.map((n) => `winget install -e --id ${n} --accept-package-agreements --accept-source-agreements`).join(' && ');
    case 'choco': return `choco install -y ${l}`;
    case 'scoop': return `scoop install ${l}`;
    default: return '';
  }
}
async function ensureBrowser(ctx) {
  const found = findChrome();
  if (found) return `Browser already available: ${found}`;
  const p = sysProbe(true), sudo = p.root || !p.have.sudo ? '' : 'sudo -n ';
  const apt = `${sudo}env DEBIAN_FRONTEND=noninteractive apt-get`;
  const attempts = [];
  if (p.isTermux || p.have.pkg) attempts.push('pkg install -y x11-repo; pkg install -y chromium');
  if (p.have['apt-get']) attempts.push(`${sudo}apt-get update -y; ${apt} install -y chromium || ${apt} install -y chromium-browser`);
  if (p.have.brew) attempts.push('brew install --cask chromium || brew install --cask google-chrome');
  if (p.have.dnf) attempts.push(`${sudo}dnf install -y chromium`);
  if (p.have.pacman) attempts.push(`${sudo}pacman -S --noconfirm chromium`);
  if (p.have.apk) attempts.push(`${sudo}apk add --no-cache chromium`);
  if (p.have.winget) attempts.push('winget install -e --id Google.Chrome --accept-package-agreements --accept-source-agreements');
  if (p.have.npx) {
    attempts.push(`npx -y playwright install ${p.have['apt-get'] && (p.root || p.have.sudo) ? '--with-deps ' : ''}chromium`);
    attempts.push(`npx -y @puppeteer/browsers install chrome-headless-shell@stable --path "${path.join(HOME, 'browsers')}"`);
  }
  const report = [];
  for (const cmd of attempts) {
    ctx?.emit?.('status', { text: `Installing a browser: ${cmd.slice(0, 70)}...` });
    const out = await shellRun(cmd, { cwd: HOME, timeout: 1200000, signal: ctx?.signal });
    chromeCache = null; probeCache = null;
    const hit = findChrome();
    report.push(`$ ${cmd}\n${out.length > 1200 ? `${out.slice(0, 400)}\n...\n${out.slice(-700)}` : out}`);
    if (hit) {
      process.env.CHROME_PATH = hit; process.env.PUPPETEER_EXECUTABLE_PATH = hit; process.env.CHROME_BIN = hit;
      return `Browser ready: ${hit}\nPass it to tools as CHROME_PATH / executablePath / browserExecutable. The screenshot tool already uses it.\n\n${report.join('\n\n')}`;
    }
  }
  return `Could not install a browser automatically.\n\n${report.join('\n\n')}\n\nRead the errors, fix the cause (missing libs, no root, disk space, wrong arch) or try another route.`;
}

// ============================================================================
// 10. MCP CLIENT
// ============================================================================
const mcpServers = new Map();
async function connectMcp(only) {
  const cfg = readJson(MCP_FILE, { servers: {} });
  for (const [name, spec] of Object.entries(cfg.servers || {})) {
    if (only && name !== only) continue;
    try {
      const child = spawn(spec.command, spec.args || [], { env: childEnv(spec.env || {}), stdio: ['pipe', 'pipe', 'pipe'], shell: IS_WIN });
      const srv = { name, child, buf: '', nextId: 1, pending: new Map(), tools: [] };
      child.stdout.on('data', (d) => {
        srv.buf += d.toString();
        let i;
        while ((i = srv.buf.indexOf('\n')) >= 0) {
          const line = srv.buf.slice(0, i).trim();
          srv.buf = srv.buf.slice(i + 1);
          if (!line.startsWith('{')) continue;
          let msg; try { msg = JSON.parse(line); } catch { continue; }
          const p = srv.pending.get(msg.id);
          if (p) { srv.pending.delete(msg.id); msg.error ? p.reject(new Error(msg.error.message)) : p.resolve(msg.result); }
        }
      });
      child.stderr.on('data', (d) => alog(`[mcp:${name}] ${d.toString().trim().slice(0, 200)}`));
      const call = (method, params) => new Promise((resolve, reject) => {
        const id = srv.nextId++;
        srv.pending.set(id, { resolve, reject });
        child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', id, method, params })}\n`);
        setTimeout(() => { if (srv.pending.delete(id)) reject(new Error(`MCP ${name}.${method} timed out`)); }, 60000);
      });
      await call('initialize', { protocolVersion: '2024-11-05', capabilities: {}, clientInfo: { name: 'power-agent', version: '3' } });
      child.stdin.write(`${JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' })}\n`);
      const list = await call('tools/list', {});
      srv.tools = list?.tools || [];
      srv.call = call;
      mcpServers.set(name, srv);
      log(`MCP server "${name}" connected (${srv.tools.length} tools).`);
    } catch (e) { warn(`MCP server "${name}" failed: ${e.message}`); }
  }
}
function mcpSchemas() {
  const out = [];
  for (const [name, srv] of mcpServers) {
    for (const t of srv.tools) {
      out.push({
        name: `mcp_${name}_${t.name}`.replace(/[^\w]/g, '_').slice(0, 60),
        description: `[MCP ${name}] ${t.description || t.name}`,
        parameters: t.inputSchema || { type: 'object', properties: {} },
        readOnly: true,
        run: async (args) => {
          const r = await srv.call('tools/call', { name: t.name, arguments: args || {} });
          return (r?.content || []).map((c) => c.text || `[${c.type}]`).join('\n') || JSON.stringify(r);
        },
      });
    }
  }
  return out;
}

// ============================================================================
// 11. TOOLS
// ============================================================================
const UA = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126 Safari/537.36';
const ENT = { amp: '&', lt: '<', gt: '>', quot: '"', apos: "'", nbsp: ' ' };
const decodeEnt = (s) => s.replace(/&(#x?[0-9a-f]+|\w+);/gi, (m, e) => {
  if (e[0] === '#') { const n = e[1].toLowerCase() === 'x' ? parseInt(e.slice(2), 16) : parseInt(e.slice(1), 10); return Number.isFinite(n) ? String.fromCodePoint(n) : m; }
  return ENT[e.toLowerCase()] ?? m;
});
const htmlToText = (h) => decodeEnt(h.replace(/<(script|style|noscript|svg|head)[\s\S]*?<\/\1>/gi, ' ')
  .replace(/<\/(p|div|h[1-6]|li|tr|section|article)>|<br\s*\/?>/gi, '\n').replace(/<[^>]+>/g, ' '))
  .replace(/[ \t]+/g, ' ').replace(/\n\s*\n+/g, '\n\n').trim();

async function webSearch(query, max) {
  const results = [];
  try {
    const r = await fetch('https://html.duckduckgo.com/html/', { method: 'POST', headers: { 'user-agent': UA, 'content-type': 'application/x-www-form-urlencoded' }, body: new URLSearchParams({ q: query }), signal: AbortSignal.timeout(15000) });
    const html = await r.text();
    for (const block of html.split(/class="[^"]*result__body/).slice(1)) {
      if (results.length >= max) break;
      const a = block.match(/<a[^>]+class="result__a"[^>]+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/);
      if (!a) continue;
      let url = decodeEnt(a[1]);
      const u = url.match(/[?&]uddg=([^&]+)/);
      if (u) url = decodeURIComponent(u[1]);
      if (url.startsWith('//')) url = `https:${url}`;
      if (/duckduckgo\.com\/y\.js/.test(url)) continue;
      const sn = block.match(/class="result__snippet"[^>]*>([\s\S]*?)<\/(a|div|td)>/);
      results.push({ title: htmlToText(a[2]), url, snippet: sn ? htmlToText(sn[1]) : '' });
    }
  } catch { /* fall back */ }
  if (results.length) return results;
  const r = await fetch(`https://en.wikipedia.org/w/api.php?action=query&list=search&format=json&srlimit=${max}&srsearch=${encodeURIComponent(query)}`, { headers: { 'user-agent': UA }, signal: AbortSignal.timeout(15000) });
  return ((await r.json()).query?.search || []).map((s) => ({ title: s.title, url: `https://en.wikipedia.org/wiki/${encodeURIComponent(s.title.replace(/ /g, '_'))}`, snippet: htmlToText(s.snippet) }));
}

const withCheckpoint = (ctx, files, label, fn) => {
  const id = checkpoint(ctx.convId, label, files);
  try { return fn(); } catch (e) { restoreCheckpoint(ctx.convId, id); throw e; }
};

const TOOLS = {
  web_search: {
    readOnly: true,
    description: 'Search the web for current information. Returns titles, URLs, snippets. Follow up with fetch_url to read the best pages.',
    parameters: { type: 'object', properties: { query: { type: 'string' }, max_results: { type: 'integer', description: '1-10, default 6' } }, required: ['query'] },
    run: async ({ query, max_results = 6 }) => { const r = await webSearch(query, Math.max(1, Math.min(10, max_results))); return r.length ? r : 'No results.'; },
  },
  fetch_url: {
    readOnly: true,
    description: 'Fetch a web page or API URL and return readable text (HTML stripped). For articles, docs, JSON APIs.',
    parameters: { type: 'object', properties: { url: { type: 'string' } }, required: ['url'] },
    run: async ({ url }) => {
      if (!/^https?:\/\//i.test(url)) throw new Error('Only http(s) URLs allowed.');
      const r = await fetch(url, { headers: { 'user-agent': UA, accept: 'text/html,application/json,text/plain,*/*' }, redirect: 'follow', signal: AbortSignal.timeout(20000) });
      const type = r.headers.get('content-type') || '', body = await r.text();
      return `HTTP ${r.status} ${type}\n\n${clip(/html/.test(type) ? htmlToText(body) : body)}`;
    },
  },
  run_command: {
    shell: true, mutating: true,
    description: 'Run a shell command in the workspace (node, python, git, npm, curl, ls, pip...). Returns exit code plus output. Default timeout 300s, max 3600s. Use background=true for dev servers and long jobs.',
    parameters: { type: 'object', properties: { command: { type: 'string' }, timeout_sec: { type: 'integer' }, background: { type: 'boolean' }, cwd: { type: 'string' } }, required: ['command'] },
    run: async ({ command, timeout_sec = 300, background = false, cwd }, ctx) => {
      if (!ctx.settings.allowShell) throw new Error('Shell access is disabled in Settings.');
      if (DENY.some((re) => re.test(command.trim()))) throw new Error('Blocked: this command would wipe the disk or shut the machine down. Use a targeted command instead.');
      return shellRun(command, { cwd: cwd ? wsPath(cwd) : WORKSPACE, timeout: Math.min(3600, timeout_sec) * 1000, signal: ctx.signal, background });
    },
  },
  run_python: {
    mutating: true,
    description: 'Run a Python snippet (writes it to a temp file in the workspace and executes it). Auto-installs nothing; use run_command with pip if needed.',
    parameters: { type: 'object', properties: { code: { type: 'string' }, timeout_sec: { type: 'integer' } }, required: ['code'] },
    run: async ({ code, timeout_sec = 300 }, ctx) => {
      if (!ctx.settings.allowShell) throw new Error('Shell access is disabled.');
      const f = wsPath(`.tmp/snippet_${newId(4)}.py`);
      fs.mkdirSync(path.dirname(f), { recursive: true });
      fs.writeFileSync(f, code);
      const py = IS_WIN ? 'python' : 'python3';
      return shellRun(`${py} "${f}"`, { timeout: Math.min(3600, timeout_sec) * 1000, signal: ctx.signal });
    },
  },
  job_output: {
    readOnly: true,
    description: 'Read buffered output of a background job.',
    parameters: { type: 'object', properties: { id: { type: 'string' }, tail_chars: { type: 'integer' } }, required: ['id'] },
    run: async ({ id, tail_chars = 8000 }) => {
      const j = jobs.get(id);
      if (!j) throw new Error('No such job.');
      return `job ${id} ${j.done ? `finished (exit ${j.code})` : 'running'}\n${j.out.slice(-tail_chars)}`;
    },
  },
  job_kill: {
    mutating: true,
    description: 'Kill a background job.',
    parameters: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'] },
    run: async ({ id }) => {
      const j = jobs.get(id);
      if (!j) throw new Error('No such job.');
      try { IS_WIN ? exec(`taskkill /pid ${j.child.pid} /T /F`) : j.child.kill('SIGKILL'); } catch { /* ignore */ }
      return `Killed ${id}.`;
    },
  },
  list_files: {
    readOnly: true,
    description: 'List a directory in the workspace (default ".").',
    parameters: { type: 'object', properties: { path: { type: 'string' } } },
    run: async ({ path: p = '.' }) => {
      const dir = wsPath(p);
      const ents = fs.readdirSync(dir, { withFileTypes: true });
      if (!ents.length) return '(empty)';
      return ents.map((d) => (d.isDirectory() ? `${d.name}/` : `${d.name} (${fs.statSync(path.join(dir, d.name)).size}B)`)).join('\n');
    },
  },
  tree: {
    readOnly: true,
    description: 'Recursive directory tree of the workspace, honoring ignore rules.',
    parameters: { type: 'object', properties: { path: { type: 'string' }, depth: { type: 'integer' } } },
    run: async ({ path: p = '.', depth = 3 }) => {
      const base = wsPath(p), out = [];
      const rec = (d, lvl) => {
        if (lvl > depth || out.length > 600) return;
        let ents; try { ents = fs.readdirSync(d, { withFileTypes: true }); } catch { return; }
        for (const e of ents) {
          if (e.name.startsWith('.') && e.name !== '.env') continue;
          const full = path.join(d, e.name);
          if (e.isDirectory()) { if (IGNORE_DIRS.has(e.name)) continue; out.push(`${'  '.repeat(lvl)}${e.name}/`); rec(full, lvl + 1); }
          else out.push(`${'  '.repeat(lvl)}${e.name}`);
        }
      };
      rec(base, 0);
      return out.join('\n') || '(empty)';
    },
  },
  glob: {
    readOnly: true,
    description: 'Find files by glob pattern (e.g. "**/*.ts", "src/**/*.test.js").',
    parameters: { type: 'object', properties: { pattern: { type: 'string' }, root: { type: 'string' }, limit: { type: 'integer' } }, required: ['pattern'] },
    run: async ({ pattern, root = '.', limit = 300 }) => {
      const found = findFiles(pattern, { root, limit: Math.min(2000, limit) });
      return found.length ? found.map(relPath).join('\n') : 'No files matched.';
    },
  },
  grep: {
    readOnly: true,
    description: 'Regex search across workspace files. Returns path:line with context. Use glob to narrow (e.g. "*.js"). literal=true for plain text.',
    parameters: { type: 'object', properties: { pattern: { type: 'string' }, glob: { type: 'string' }, root: { type: 'string' }, ignore_case: { type: 'boolean' }, literal: { type: 'boolean' }, limit: { type: 'integer' }, context_lines: { type: 'integer' } }, required: ['pattern'] },
    run: async ({ pattern, glob, root = '.', ignore_case, literal, limit = 200, context_lines = 0 }) =>
      grepTree({ pattern, glob, root, ignoreCase: ignore_case, literal, limit: Math.min(1000, limit), contextLines: context_lines }).join('\n') || 'No matches.',
  },
  symbol_find: {
    readOnly: true,
    description: 'Find where a function/class/type/method is DEFINED by exact name.',
    parameters: { type: 'object', properties: { name: { type: 'string' } }, required: ['name'] },
    run: async ({ name }) => symbolFind(name).join('\n') || 'No definition found.',
  },
  repo_map: {
    readOnly: true,
    description: 'Compact symbol index of the whole workspace (files + top-level declarations). Cheapest way to orient yourself in a new codebase.',
    parameters: { type: 'object', properties: {} },
    run: async () => repoMap() || '(empty workspace)',
  },
  read_file: {
    readOnly: true,
    description: 'Read a text file with line numbers. Use offset/limit for big files.',
    parameters: { type: 'object', properties: { path: { type: 'string' }, offset: { type: 'integer', description: '1-based start line' }, limit: { type: 'integer', description: 'lines to read, default 2000' } }, required: ['path'] },
    run: async ({ path: p, offset = 1, limit = 2000 }) => {
      const f = wsPath(p);
      const buf = fs.readFileSync(f);
      if (isBinary(buf)) return `[binary file, ${buf.length} bytes]`;
      const all = buf.toString('utf8').replace(/\r\n/g, '\n').split('\n');
      const slice = all.slice(Math.max(0, offset - 1), Math.max(0, offset - 1) + Math.min(6000, limit));
      const body = slice.map((l, i) => `${String(offset + i).padStart(5)}│ ${l}`).join('\n');
      return clip(`${relPath(f)} (${all.length} lines)\n${body}${offset - 1 + slice.length < all.length ? `\n...[${all.length - (offset - 1 + slice.length)} more lines]` : ''}`, 40000);
    },
  },
  batch_read: {
    readOnly: true,
    description: 'Read several files in one call. Cheaper than sequential read_file. paths = array of relative paths.',
    parameters: { type: 'object', properties: { paths: { type: 'array', items: { type: 'string' } }, limit: { type: 'integer' } }, required: ['paths'] },
    run: async ({ paths, limit = 800 }) => {
      const parts = [];
      for (const p of (paths || []).slice(0, 25)) {
        try {
          const f = wsPath(p), buf = fs.readFileSync(f);
          if (isBinary(buf)) { parts.push(`=== ${p} ===\n[binary ${buf.length}B]`); continue; }
          const lines = buf.toString('utf8').replace(/\r\n/g, '\n').split('\n');
          parts.push(`=== ${p} (${lines.length} lines) ===\n${lines.slice(0, limit).map((l, i) => `${String(i + 1).padStart(5)}│ ${l}`).join('\n')}${lines.length > limit ? `\n...[${lines.length - limit} more]` : ''}`);
        } catch (e) { parts.push(`=== ${p} ===\nERROR: ${e.message}`); }
      }
      return clip(parts.join('\n\n'), 60000);
    },
  },
  create_file: {
    mutating: true,
    description: 'Create a new file (fails if it exists unless overwrite=true). Parent folders are created.',
    parameters: { type: 'object', properties: { path: { type: 'string' }, content: { type: 'string' }, overwrite: { type: 'boolean' } }, required: ['path', 'content'] },
    run: async ({ path: p, content = '', overwrite }, ctx) => {
      const f = wsPath(p);
      if (fs.existsSync(f) && !overwrite) throw new Error(`${p} already exists (use apply_patch or overwrite=true).`);
      return withCheckpoint(ctx, [f], `create ${p}`, () => {
        fs.mkdirSync(path.dirname(f), { recursive: true });
        fs.writeFileSync(f, content);
        mapCache.at = 0;
        const hook = runHooks('afterEdit', f);
        return `Created ${relPath(f)} (${Buffer.byteLength(content)} bytes). ${fileUrl(f)}${hook ? `\n${hook}` : ''}`;
      });
    },
  },
  apply_patch: {
    mutating: true,
    description: `Edit existing files with exact search/replace blocks. PREFERRED way to change code.
Format (path on its own line, then blocks):
  src/app.js
  <<<<<<< SEARCH
  old exact lines
  =======
  new lines
  >>>>>>> REPLACE
Rules: SEARCH must be unique-ish and copied verbatim from read_file (indentation matters, whitespace is fuzzy-matched). Empty SEARCH = prepend. Multiple files and multiple blocks per call are allowed. Nothing is written unless every block matches.`,
    parameters: { type: 'object', properties: { patch: { type: 'string' } }, required: ['patch'] },
    run: async ({ patch }, ctx) => {
      const ops = parsePatch(patch);
      if (!ops.length) throw new Error('No valid SEARCH/REPLACE blocks found.');
      const byFile = new Map();
      for (const op of ops) {
        const f = wsPath(op.file);
        if (!byFile.has(f)) byFile.set(f, []);
        byFile.get(f).push(op);
      }
      const files = [...byFile.keys()];
      const before = new Map(files.map((f) => [f, fs.existsSync(f) ? fs.readFileSync(f, 'utf8') : '']));
      const id = checkpoint(ctx.convId, `patch ${files.map(relPath).join(', ')}`, files);
      const report = [];
      try {
        for (const [f, list] of byFile) report.push(`${relPath(f)}: ${applyOpsToFile(f, list)}`);
      } catch (e) {
        restoreCheckpoint(ctx.convId, id);
        throw new Error(`${e.message}\n\nAll changes rolled back.`);
      }
      mapCache.at = 0;
      const diffs = [];
      let add = 0, del = 0;
      for (const f of files) {
        const after = fs.readFileSync(f, 'utf8');
        const st = diffStat(before.get(f), after);
        add += st.add; del += st.del;
        if (st.add || st.del) diffs.push(previewDiff(before.get(f), after, f));
        const hook = runHooks('afterEdit', f);
        if (hook) report.push(`hook: ${hook}`);
      }
      return `${report.join('\n')}\n+${add} -${del}\n\n${clip(diffs.join('\n\n'), 6000)}`;
    },
  },
  multi_edit: {
    mutating: true,
    description: 'Apply several search/replace edits in one atomic call. edits = [{path, search, replace}].',
    parameters: { type: 'object', properties: { edits: { type: 'array', items: { type: 'object', properties: { path: { type: 'string' }, search: { type: 'string' }, replace: { type: 'string' } }, required: ['path', 'search', 'replace'] } } }, required: ['edits'] },
    run: async ({ edits }, ctx) => {
      const patch = (edits || []).map((e) => `${e.path}\n<<<<<<< SEARCH\n${e.search}\n=======\n${e.replace}\n>>>>>>> REPLACE`).join('\n');
      return TOOLS.apply_patch.run({ patch }, ctx);
    },
  },
  delete_path: {
    mutating: true,
    description: 'Delete a file or directory (recursive) inside the workspace.',
    parameters: { type: 'object', properties: { path: { type: 'string' } }, required: ['path'] },
    run: async ({ path: p }, ctx) => {
      const f = wsPath(p);
      if (!fs.existsSync(f)) throw new Error('Not found.');
      const files = fs.statSync(f).isDirectory() ? [...walk(f)] : [f];
      return withCheckpoint(ctx, files, `delete ${p}`, () => { fs.rmSync(f, { recursive: true, force: true }); mapCache.at = 0; return `Deleted ${p}.`; });
    },
  },
  move_path: {
    mutating: true,
    description: 'Move or rename a file/directory.',
    parameters: { type: 'object', properties: { from: { type: 'string' }, to: { type: 'string' } }, required: ['from', 'to'] },
    run: async ({ from, to }, ctx) => {
      const a = wsPath(from), b = wsPath(to);
      if (!fs.existsSync(a)) throw new Error('Source not found.');
      return withCheckpoint(ctx, [a, b], `move ${from} -> ${to}`, () => {
        fs.mkdirSync(path.dirname(b), { recursive: true });
        fs.renameSync(a, b);
        mapCache.at = 0;
        return `Moved ${from} -> ${to}.`;
      });
    },
  },
  todo_write: {
    description: 'Maintain the task list for this conversation. Call it first for any multi-step job, and update status as you go (pending / in_progress / done). Keep exactly one item in_progress.',
    parameters: { type: 'object', properties: { todos: { type: 'array', items: { type: 'object', properties: { content: { type: 'string' }, status: { type: 'string', enum: ['pending', 'in_progress', 'done'] } }, required: ['content', 'status'] } } }, required: ['todos'] },
    run: async ({ todos }, ctx) => {
      ctx.conv.todos = (todos || []).slice(0, 40);
      saveConversation(ctx.conv);
      ctx.emit('todos', { todos: ctx.conv.todos });
      const mark = { pending: '[ ]', in_progress: '[~]', done: '[x]' };
      return ctx.conv.todos.map((t) => `${mark[t.status] || '[ ]'} ${t.content}`).join('\n') || '(empty)';
    },
  },
  todo_read: {
    readOnly: true,
    description: 'Read the current task list.',
    parameters: { type: 'object', properties: {} },
    run: async (_a, ctx) => (ctx.conv.todos || []).map((t) => `${t.status}: ${t.content}`).join('\n') || '(empty)',
  },
  spawn_agent: {
    parallel: true,
    description: 'Delegate a self-contained sub-task to a fresh subagent with its own context and tool loop. Great for parallel exploration ("map how auth works", "find every caller of X", "write tests for Y"). The subagent only sees what you put in the task string, so be explicit. Returns its final report. Call it several times in ONE turn to run subagents in parallel.',
    parameters: { type: 'object', properties: { task: { type: 'string', description: 'Full instructions, incl. files/paths to look at and exactly what to return' }, tools: { type: 'array', items: { type: 'string' }, description: 'Optional tool allowlist' } }, required: ['task'] },
    run: async ({ task, tools }, ctx) => runSubAgent(task, { ...ctx, toolAllow: tools, depth: (ctx.depth || 0) + 1 }),
  },
  git: {
    mutating: true,
    description: 'Run a git command in the workspace. Safe subcommands only: status, diff, log, show, add, commit, branch, checkout, switch, stash, restore, rev-parse, blame, remote, tag, fetch, pull, push.',
    parameters: { type: 'object', properties: { args: { type: 'string', description: 'e.g. "status --short" or "diff HEAD~1 -- src/"' } }, required: ['args'] },
    run: async ({ args }) => {
      const sub = String(args).trim().split(/\s+/)[0];
      const ok = ['status', 'diff', 'log', 'show', 'add', 'commit', 'branch', 'checkout', 'switch', 'stash', 'restore', 'rev-parse', 'blame', 'remote', 'tag', 'fetch', 'pull', 'push', 'init'];
      if (!ok.includes(sub)) throw new Error(`git ${sub} not allowed.`);
      return shellRun(`git ${args}`, { cwd: WORKSPACE, timeout: 60000 });
    },
  },
  run_tests: {
    shell: true, mutating: true,
    description: 'Detect and run the project test/lint command (package.json scripts, pytest, cargo, go). Optional script name.',
    parameters: { type: 'object', properties: { script: { type: 'string', description: 'npm script name, e.g. "test" or "lint"' } } },
    run: async ({ script }, ctx) => {
      if (!ctx.settings.allowShell) throw new Error('Shell access is disabled.');
      const pkgPath = path.join(WORKSPACE, 'package.json');
      let cmd = null;
      if (fs.existsSync(pkgPath)) {
        const scripts = readJson(pkgPath, {}).scripts || {};
        const pick = script && scripts[script] ? script : ['test', 'lint', 'check', 'typecheck'].find((s) => scripts[s]);
        if (pick) cmd = `npm run ${pick} --silent`;
      }
      if (!cmd) {
        if (fs.existsSync(path.join(WORKSPACE, 'pyproject.toml')) || fs.existsSync(path.join(WORKSPACE, 'pytest.ini'))) cmd = 'pytest -q';
        else if (fs.existsSync(path.join(WORKSPACE, 'Cargo.toml'))) cmd = 'cargo test';
        else if (fs.existsSync(path.join(WORKSPACE, 'go.mod'))) cmd = 'go test ./...';
      }
      if (!cmd) throw new Error('No test command detected.');
      return shellRun(cmd, { timeout: 300000, signal: ctx.signal });
    },
  },
  screenshot: {
    mutating: true,
    description: 'Screenshot a URL or local HTML file with headless Chrome (scripts run). Returns markdown image. If Chrome is missing, install_package ["chromium"] sets it up.',
    parameters: { type: 'object', properties: { url: { type: 'string', description: 'http(s) URL or workspace file path' }, width: { type: 'integer' }, height: { type: 'integer' }, full_page: { type: 'boolean' } }, required: ['url'] },
    run: async ({ url, width = 1280, height = 900, full_page }) => {
      const CHROME = findChrome();
      if (!CHROME) throw new Error('No Chrome/Chromium found. Call install_package with names ["chromium"]: it tries apt/pkg/brew/playwright automatically. Then retry.');
      const target = /^https?:\/\//i.test(url) ? url : (previewUp ? `http://127.0.0.1:${CFG.previewMode === 'inline' ? CFG.agentPort : CFG.previewPort}/run/${PREVIEW_TOKEN}/${relPath(wsPath(url)).split('/').map(encodeURIComponent).join('/')}` : `file://${wsPath(url)}`);
      const out = wsPath(`images/shot_${newId(5)}.png`);
      fs.mkdirSync(path.dirname(out), { recursive: true });
      const args = ['--headless=new', '--disable-gpu', '--hide-scrollbars', '--no-sandbox', '--disable-dev-shm-usage', '--enable-unsafe-swiftshader', '--virtual-time-budget=4000', `--window-size=${width},${height}`, `--screenshot=${out}`];
      if (full_page) args.push('--screenshot-format=png');
      args.push(target);
      await new Promise((resolve, reject) => {
        const p = spawn(CHROME, args, { stdio: 'ignore' });
        p.on('error', reject);
        p.on('close', () => resolve());
        setTimeout(() => { try { p.kill('SIGKILL'); } catch { /* ignore */ } resolve(); }, 45000);
      });
      if (!fs.existsSync(out)) throw new Error('Screenshot failed (page may not have loaded).');
      mapCache.at = 0;
      return `Screenshot of ${url}\n\n![screenshot](${fileUrl(out)})`;
    },
  },
  deps_audit: {
    readOnly: true,
    description: 'Audit dependencies: npm audit summary, outdated packages, and lockfile presence.',
    parameters: { type: 'object', properties: {} },
    run: async () => {
      const out = [];
      const pkg = path.join(WORKSPACE, 'package.json');
      if (fs.existsSync(pkg)) {
        out.push(`package.json deps: ${Object.keys(readJson(pkg, {}).dependencies || {}).length}, dev: ${Object.keys(readJson(pkg, {}).devDependencies || {}).length}`);
        out.push(`lockfile: ${['package-lock.json', 'pnpm-lock.yaml', 'yarn.lock'].filter((f) => fs.existsSync(path.join(WORKSPACE, f))).join(', ') || 'NONE'}`);
        out.push(clip(await shellRun('npm outdated --json', { timeout: 60000 }), 3000));
        out.push(clip(await shellRun('npm audit --json', { timeout: 90000 }), 3000));
      } else if (fs.existsSync(path.join(WORKSPACE, 'requirements.txt'))) {
        out.push(fs.readFileSync(path.join(WORKSPACE, 'requirements.txt'), 'utf8').slice(0, 2000));
        out.push(clip(await shellRun('pip list --outdated', { timeout: 60000 }), 3000));
      } else out.push('No package.json or requirements.txt.');
      return out.join('\n\n');
    },
  },
  port_check: {
    readOnly: true,
    description: 'Check whether a TCP port is open on a host (default 127.0.0.1).',
    parameters: { type: 'object', properties: { port: { type: 'integer' }, host: { type: 'string' } }, required: ['port'] },
    run: async ({ port, host = '127.0.0.1' }) => new Promise((resolve) => {
      const s = net.connect({ port, host });
      const done = (t) => { try { s.destroy(); } catch { /* ignore */ } resolve(t); };
      s.setTimeout(4000);
      s.on('connect', () => done(`open: ${host}:${port}`));
      s.on('timeout', () => done(`timeout: ${host}:${port}`));
      s.on('error', (e) => done(`closed: ${host}:${port} (${e.code})`));
    }),
  },
  rewind: {
    mutating: true,
    description: 'Restore files from a checkpoint. Omit id for the most recent one. Use after a bad edit.',
    parameters: { type: 'object', properties: { id: { type: 'string' } } },
    run: async ({ id }, ctx) => {
      const list = listCheckpoints(ctx.convId);
      const target = id || list[0]?.id;
      if (!target) throw new Error('No checkpoints yet.');
      const files = restoreCheckpoint(ctx.convId, target);
      mapCache.at = 0;
      return `Restored ${files.length} file(s) from checkpoint ${target}: ${files.join(', ')}`;
    },
  },
  remember: {
    description: 'Save a durable fact to long-term memory (name, preferences, stack, decisions). Recalled in future chats. Never store secrets.',
    parameters: { type: 'object', properties: { text: { type: 'string' }, tags: { type: 'array', items: { type: 'string' } } }, required: ['text'] },
    run: async ({ text, tags = [] }) => `Remembered (id ${addMemory(text, tags).id}).`,
  },
  recall: {
    readOnly: true,
    description: 'Search long-term memory. Empty query = most recent.',
    parameters: { type: 'object', properties: { query: { type: 'string' } } },
    run: async ({ query = '' }) => { const r = query.trim() ? searchMemories(query, 15) : listMemories().slice(-15); return r.length ? r.map((m) => `[${m.id}] ${m.text}`).join('\n') : 'No matching memories.'; },
  },
  forget: {
    mutating: true,
    description: 'Delete a memory by id.',
    parameters: { type: 'object', properties: { id: { type: 'string' } }, required: ['id'] },
    run: async ({ id }) => (deleteMemory(id) ? 'Forgotten.' : 'No memory with that id.'),
  },
  generate_image: {
    mutating: true,
    description: 'Generate an image from a prompt. Returns markdown to include in your answer.',
    parameters: { type: 'object', properties: { prompt: { type: 'string' }, size: { type: 'string' } }, required: ['prompt'] },
    run: async ({ prompt, size = '1024x1024' }, ctx) => {
      const j = await ctx.llm.post('/images/generations', { model: ctx.settings.imageModel || 'auto', prompt, n: 1, size, response_format: 'b64_json' });
      const item = j.data?.[0];
      if (!item) throw new Error('No image returned.');
      const buf = item.b64_json ? Buffer.from(item.b64_json, 'base64') : Buffer.from(await (await fetch(item.url)).arrayBuffer());
      const f = wsPath(`images/${newId()}.png`);
      fs.mkdirSync(path.dirname(f), { recursive: true });
      fs.writeFileSync(f, buf);
      mapCache.at = 0;
      return `Image ready: ![image](${fileUrl(f)})`;
    },
  },
  think: {
    readOnly: true,
    description: 'Private scratchpad: reason step by step, plan, compare options, double-check before a risky or complex action. No side effects, no length limit. Think as long as you need.',
    parameters: { type: 'object', properties: { thought: { type: 'string' } }, required: ['thought'] },
    run: async ({ thought }) => `Noted (${String(thought || '').length} chars). Continue.`,
  },
  env_info: {
    readOnly: true,
    description: 'Inspect the machine you run on: OS, CPU arch, Termux/Android, root/sudo, package managers, which tools (ffmpeg, python, git, docker, chrome...) are installed, RAM, workspace. Call it whenever a task needs a tool you are not sure exists.',
    parameters: { type: 'object', properties: {} },
    run: async () => JSON.stringify(sysProbe(true), null, 2),
  },
  install_package: {
    shell: true, mutating: true,
    description: 'Install software yourself. names: packages to install. via: "auto" (system package manager: pkg/apt/brew/dnf/pacman/apk/winget/choco/scoop), "npm", or "pip". Special names chromium / chrome / browser run a multi-route headless-browser setup (apt, pkg, brew, playwright, puppeteer) until a browser works. Verifies the result. Never tell the user a tool is unavailable before trying this.',
    parameters: { type: 'object', properties: { names: { type: 'array', items: { type: 'string' } }, via: { type: 'string', enum: ['auto', 'npm', 'pip'] }, global: { type: 'boolean', description: 'npm only: -g (default true)' } }, required: ['names'] },
    run: async ({ names, via = 'auto', global: isGlobal = true }, ctx) => {
      const list = (Array.isArray(names) ? names : String(names || '').split(/[\s,]+/)).filter(Boolean);
      if (!list.length) throw new Error('names is required');
      if (list.some((n) => !/^[A-Za-z0-9._+:@/=-]+$/.test(n))) throw new Error('Invalid package name.');
      const BROWSER = /^(chrome|chromium|chromium-browser|google-chrome|headless-chrome|browser)$/i;
      const rest = list.filter((n) => !BROWSER.test(n)), outs = [];
      if (rest.length !== list.length) outs.push(await ensureBrowser(ctx));
      if (rest.length) {
        const p = sysProbe(true), sudo = p.root || !p.have.sudo ? '' : 'sudo -n ', py = p.have.python3 ? 'python3' : 'python';
        let cmd;
        if (via === 'npm') cmd = `npm install ${isGlobal ? '-g ' : ''}${rest.join(' ')}`;
        else if (via === 'pip') cmd = `${py} -m pip install --break-system-packages ${rest.join(' ')} || ${py} -m pip install ${rest.join(' ')}`;
        else {
          const m = pickInstaller(p);
          if (!m) throw new Error('No system package manager found. Use via "npm"/"pip", or download a binary with http_request.');
          cmd = installCmd(m, rest.map((n) => mapPkg(m, n)), sudo);
        }
        ctx.emit?.('status', { text: `Installing ${rest.join(', ')}...` });
        outs.push(`$ ${cmd}\n${await shellRun(cmd, { cwd: WORKSPACE, timeout: 1800000, signal: ctx.signal })}`);
      }
      probeCache = null;
      const check = rest.map((n) => `${n}: ${which(n.split(/[=@:]/)[0]) || 'not on PATH (may be a library, or needs the full path)'}`);
      return clip(`${outs.join('\n\n')}\n\nVerification:\n${check.join('\n') || '(browser only)'}`, 20000);
    },
  },
  http_request: {
    mutating: true,
    description: 'Full HTTP client: any method, headers, JSON or text body. Set save_to to stream the response (any size, binary ok) into a workspace file: models, videos, zips, images, binaries. Returns status, headers, and the body text (or the saved path and size).',
    parameters: { type: 'object', properties: { url: { type: 'string' }, method: { type: 'string' }, headers: { type: 'object' }, body: { type: 'string' }, json: { type: 'object', description: 'sent as JSON body' }, save_to: { type: 'string', description: 'workspace path to save the response to' }, timeout_sec: { type: 'integer' } }, required: ['url'] },
    run: async ({ url, method = 'GET', headers = {}, body, json, save_to, timeout_sec = 120 }, ctx) => {
      if (!/^https?:\/\//i.test(url)) throw new Error('Only http(s) URLs allowed.');
      const m = String(method).toUpperCase(), h = { 'user-agent': UA, ...headers };
      let payload = body;
      if (json !== undefined) { payload = JSON.stringify(json); if (!h['content-type']) h['content-type'] = 'application/json'; }
      const r = await fetch(url, { method: m, headers: h, body: m === 'GET' || m === 'HEAD' ? undefined : payload, redirect: 'follow', signal: anySignal([ctx.signal, AbortSignal.timeout(Math.min(3600, timeout_sec) * 1000)].filter(Boolean)) });
      const head = `HTTP ${r.status} ${r.statusText}\n${[...r.headers].slice(0, 25).map(([k, v]) => `${k}: ${v}`).join('\n')}`;
      if (save_to) {
        const f = wsPath(save_to);
        fs.mkdirSync(path.dirname(f), { recursive: true });
        if (r.body) await pipeline(Readable.fromWeb(r.body), fs.createWriteStream(f));
        mapCache.at = 0;
        return `${head}\n\nSaved ${fs.statSync(f).size} bytes to ${relPath(f)}`;
      }
      const type = r.headers.get('content-type') || '', text = await r.text();
      return `${head}\n\n${clip(/html/.test(type) ? htmlToText(text) : text, 60000)}`;
    },
  },
  define_tool: {
    shell: true, mutating: true,
    description: 'Create a PERSISTENT custom tool for any capability that is missing. code is the body of an async function (args, ctx, h) that returns a string or JSON value. h provides: fs, path, os, crypto, spawn, run(cmd,{cwd,timeout}), fetch, wsPath, WORKSPACE, sleep. The tool is callable from the next step and survives restarts.',
    parameters: { type: 'object', properties: { name: { type: 'string', description: 'snake_case, 3-41 chars' }, description: { type: 'string' }, parameters: { type: 'object', description: 'JSON schema of args' }, code: { type: 'string' } }, required: ['name', 'description', 'code'] },
    run: async ({ name, description, parameters, code }) => {
      if (!/^[a-z][a-z0-9_]{2,40}$/.test(name || '')) throw new Error('name must be snake_case, 3-41 chars');
      if (TOOLS[name]) throw new Error('name collides with a built-in tool');
      try { new AsyncFunction('args', 'ctx', 'h', code); } catch (e) { throw new Error(`code does not compile: ${e.message}`); }
      const list = readJson(CUSTOM_FILE, []).filter((t) => t.name !== name);
      list.push({ name, description: String(description || name).slice(0, 500), parameters: parameters && typeof parameters === 'object' ? parameters : { type: 'object', properties: {} }, code: String(code) });
      writeJson(CUSTOM_FILE, list);
      return `Custom tool "${name}" saved. Call it from the next step.`;
    },
  },
  remove_tool: {
    mutating: true,
    description: 'Delete a custom tool created with define_tool.',
    parameters: { type: 'object', properties: { name: { type: 'string' } }, required: ['name'] },
    run: async ({ name }) => { const list = readJson(CUSTOM_FILE, []); writeJson(CUSTOM_FILE, list.filter((t) => t.name !== name)); return list.some((t) => t.name === name) ? `Removed ${name}.` : `No custom tool named ${name}.`; },
  },
  mcp_add: {
    shell: true, mutating: true,
    description: 'Connect an MCP server (stdio) and persist it. Example: command "npx", args ["-y","@modelcontextprotocol/server-filesystem","."]. Its tools show up next step as mcp_<name>_<tool>.',
    parameters: { type: 'object', properties: { name: { type: 'string' }, command: { type: 'string' }, args: { type: 'array', items: { type: 'string' } }, env: { type: 'object' } }, required: ['name', 'command'] },
    run: async ({ name, command, args = [], env = {} }) => {
      if (!/^[\w-]{1,30}$/.test(name || '')) throw new Error('bad server name');
      const cfg = readJson(MCP_FILE, { servers: {} }); cfg.servers = cfg.servers || {};
      cfg.servers[name] = { command, args, env }; writeJson(MCP_FILE, cfg);
      if (mcpServers.has(name)) { try { mcpServers.get(name).child.kill(); } catch { /* ignore */ } mcpServers.delete(name); }
      await connectMcp(name);
      const srv = mcpServers.get(name);
      return srv ? `MCP "${name}" connected, ${srv.tools.length} tools: ${srv.tools.map((t) => t.name).join(', ')}. Use them next step as mcp_${name}_<tool>.` : 'Saved, but the server failed to start. Test the command with run_command first.';
    },
  },
  get_datetime: {
    readOnly: true,
    description: 'Current date, time and timezone.',
    parameters: { type: 'object', properties: {} },
    run: async () => new Date().toString(),
  },
};

const CUSTOM_FILE = path.join(HOME, 'custom-tools.json');
const AsyncFunction = Object.getPrototypeOf(async function () { /* probe */ }).constructor;
function customTools() {
  const out = {};
  for (const t of readJson(CUSTOM_FILE, [])) {
    if (!t?.name || TOOLS[t.name]) continue;
    let fn; try { fn = new AsyncFunction('args', 'ctx', 'h', t.code); } catch { continue; }
    out[t.name] = {
      shell: true, mutating: true,
      description: `[custom] ${t.description}`,
      parameters: t.parameters || { type: 'object', properties: {} },
      run: async (args, ctx) => {
        const h = { fs, path, os, crypto, spawn, fetch, wsPath, WORKSPACE, sleep, run: (cmd, o = {}) => shellRun(cmd, { timeout: 600000, signal: ctx?.signal, ...o }) };
        const r = await fn(args || {}, ctx, h);
        return typeof r === 'string' ? r : JSON.stringify(r ?? 'ok', null, 2);
      },
    };
  }
  return out;
}
function allTools(settings) {
  const out = {};
  for (const [name, t] of Object.entries(TOOLS)) {
    if (t.shell && !settings.allowShell) continue;
    if ((settings.disabledTools || []).includes(name)) continue;
    out[name] = t;
  }
  for (const t of mcpSchemas()) out[t.name] = t;
  if (settings.allowShell) Object.assign(out, customTools());
  return out;
}
function toolSchemas(settings, allow) {
  const tools = allTools(settings);
  return Object.entries(tools)
    .filter(([n]) => !allow || allow.includes(n))
    .map(([name, t]) => ({ type: 'function', function: { name, description: t.description, parameters: t.parameters } }));
}

const PLAN_OK = new Set(['todo_write', 'spawn_agent', 'remember']);
const ASK_EXEMPT = PLAN_OK;
async function runTool(name, raw, ctx) {
  const tools = allTools(ctx.settings);
  const t = tools[name];
  if (!t) return `Error: unknown tool "${name}". Available: ${Object.keys(tools).join(', ')}`;
  let args;
  try { args = typeof raw === 'string' ? (raw.trim() ? JSON.parse(raw) : {}) : raw || {}; } catch { return 'Error: arguments were not valid JSON. Retry with valid JSON.'; }
  const mode = ctx.settings.mode;
  if (mode === 'plan' && !t.readOnly && !PLAN_OK.has(name)) return `Blocked: PLAN MODE is read-only and "${name}" would change things. Finish investigating, then present your plan.`;
  // approvals only exist in 'ask' (and shell in 'acceptEdits'). Default mode 'yolo' never prompts.
  const needsApproval = !t.readOnly && !ASK_EXEMPT.has(name) && (mode === 'ask' || (mode === 'acceptEdits' && t.shell)) &&
    !(ctx.settings.autoApprove || []).includes(name);
  if (needsApproval) {
    const ok = await ctx.approve(name, args);
    if (!ok) return `Denied by user: ${name}`;
  }
  try {
    const out = await t.run(args, ctx);
    return typeof out === 'string' ? clip(out, 60000) : clip(JSON.stringify(out, null, 2), 60000);
  } catch (e) { return `Error: ${e.message}`; }
}

// ============================================================================
// 12. SUBAGENTS
// ============================================================================
const sleep = (ms, signal) => new Promise((resolve, reject) => {
  const t = setTimeout(resolve, ms);
  signal?.addEventListener('abort', () => { clearTimeout(t); reject(new Error('Stopped.')); }, { once: true });
});
const isTransient = (e) => e.status === 429 || e.status >= 500 ||
  (!e.status && /fetch failed|network|timeout|terminated|socket|ECONN|ETIMEDOUT|EAI_AGAIN/i.test(`${e.message} ${e.cause?.code || ''}`));
const isOverflow = (e) => [400, 413, 422].includes(e.status) && /context|too long|too many tokens|maximum|exceed|reduce the length|token limit/i.test(e.message);
async function withRetry(fn, signal, tries = 6) {
  for (let n = 0; ; n++) {
    try { return await fn(); }
    catch (e) { if (signal?.aborted || n >= tries || !isTransient(e)) throw e; await sleep(Math.min(30000, 1000 * 2 ** n), signal); }
  }
}

async function runSubAgent(task, ctx) {
  const depth = ctx.depth || 0;
  if (depth > 3) throw new Error('Subagent nesting too deep.');
  const settings = { ...ctx.settings, mode: ctx.settings.mode === 'plan' ? 'plan' : 'yolo' };
  const limit = settings.maxSteps > 0 ? settings.maxSteps : Infinity;
  const llm = ctx.llm;
  const tools = toolSchemas(settings, ctx.toolAllow || null).filter((t) => depth < 3 || t.function.name !== 'spawn_agent');
  const messages = [
    { role: 'system', content: `You are a focused subagent inside a larger task. You are fully autonomous: never ask for permission, use your tools, verify your work, and finish with a compact report the parent can act on (what you found or changed, file paths, how you verified). No pleasantries. Today: ${new Date().toISOString()}` },
    { role: 'user', content: task },
  ];
  const sub = { ...ctx, settings, depth };
  const budget = learnedBudget.get(settings.model) || effBudget(settings);
  let lastSig = '', same = 0;
  for (let step = 0; step < limit; step++) {
    if (ctx.signal?.aborted) throw new Error('Stopped.');
    if (estTokens(JSON.stringify(messages)) > budget) {
      for (let i = 2; i < messages.length - 10; i++) {
        const m = messages[i];
        if (m.role === 'tool' && m.content.length > 1500) messages[i] = { ...m, content: `${m.content.slice(0, 1500)}\n...[old output trimmed]` };
      }
    }
    const res = await withRetry(() => llm.post('/chat/completions', {
      model: settings.model, temperature: 0.2, messages, ...(tools.length ? { tools, tool_choice: 'auto' } : {}),
    }, ctx.signal), ctx.signal);
    const msg = res.choices?.[0]?.message || {};
    const calls = (msg.tool_calls || []).filter((c) => c.function?.name);
    if (!calls.length) return clip(msg.content || '(subagent returned nothing)', 12000);
    messages.push({ role: 'assistant', content: msg.content || '', tool_calls: calls });
    let sig = '';
    for (const c of calls) {
      const out = await runTool(c.function.name, c.function.arguments, sub);
      sig += c.function.name + c.function.arguments + out.slice(0, 200);
      messages.push({ role: 'tool', tool_call_id: c.id, content: out });
    }
    same = sig === lastSig ? same + 1 : 0; lastSig = sig;
    if (same >= 6) break; // stuck repeating itself
  }
  const last = await withRetry(() => llm.post('/chat/completions', { model: settings.model, temperature: 0.2, messages: [...messages, { role: 'user', content: 'Stop now and report your findings.' }] }, ctx.signal), ctx.signal);
  return clip(last.choices?.[0]?.message?.content || '(subagent produced no report)', 12000);
}

// ============================================================================
// 13. LLM CLIENT
// ============================================================================
function llmClient(settings) {
  const base = String(settings.baseUrl || LLM_BASE).replace(/\/+$/, '');
  const custom = EXTERNAL_LLM || !!settings.baseUrl, who = custom ? 'The LLM API' : 'FreeLLMAPI';
  const headers = { 'content-type': 'application/json', authorization: `Bearer ${settings.apiKey}` };
  const guard = () => {
    if (!custom && !USE_FREELLM) throw new Error('No LLM endpoint configured. Open Settings and set the API base URL (and key), or set LLM_BASE_URL on the server.');
    if (!custom && bootState.phase !== 'ready') throw new Error(bootState.phase === 'error' ? `FreeLLMAPI failed to start: ${bootState.message}` : `FreeLLMAPI is still starting (${bootState.message}) Try again in a minute.`);
  };
  const doFetch = async (p, body, signal) => {
    if (!settings.apiKey) throw new Error(custom ? 'No API key yet. Open Settings and paste your provider API key (or set LLM_API_KEY).' : 'No FreeLLMAPI key yet. Open Settings and paste the unified key from the FreeLLMAPI dashboard (Keys page).');
    if (custom && /^auto(:|$)/.test(String(body?.model || ''))) throw new Error('Pick a real model first (model menu or Settings, or set LLM_MODEL). "auto" only exists in FreeLLMAPI.');
    guard();
    const r = await fetch(`${base}${p}`, {
      method: 'POST', headers, body: JSON.stringify(body),
      signal: signal ? anySignal([signal, AbortSignal.timeout(900000)]) : AbortSignal.timeout(900000),
    });
    if (!r.ok) {
      const text = await r.text();
      let j = null; try { j = JSON.parse(text); } catch { /* ignore */ }
      const e = new Error(j?.error?.message || text.slice(0, 400) || `HTTP ${r.status}`);
      e.status = r.status;
      throw e;
    }
    return r;
  };
  return {
    async post(p, body, signal) {
      const r = await doFetch(p, body, signal);
      const text = await r.text();
      let j = null; try { j = JSON.parse(text); } catch { /* ignore */ }
      if (!j) throw new Error(`${who} returned a non-JSON response.`);
      j._routedVia = r.headers.get('x-routed-via');
      return j;
    },
    async get(p) {
      guard();
      const r = await fetch(`${base}${p}`, { headers, signal: AbortSignal.timeout(15000) });
      if (!r.ok) throw new Error(`HTTP ${r.status}: ${(await r.text()).slice(0, 300)}`);
      return r.json();
    },
    async stream(body, signal, onEvent) {
      const r = await doFetch('/chat/completions', { ...body, stream: true }, signal);
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let nl;
        while ((nl = buf.indexOf('\n')) >= 0) {
          const line = buf.slice(0, nl).trim();
          buf = buf.slice(nl + 1);
          if (!line.startsWith('data:')) continue;
          const payload = line.slice(5).trim();
          if (payload === '[DONE]') return;
          let j; try { j = JSON.parse(payload); } catch { continue; }
          const fr = j.choices?.[0]?.finish_reason;
          if (fr) onEvent('finish', fr);
          const delta = j.choices?.[0]?.delta;
          if (!delta) continue;
          if (delta.content) onEvent('content', delta.content);
          if (delta.tool_calls) for (const tc of delta.tool_calls) onEvent('tool', tc);
        }
      }
    },
  };
}

// ============================================================================
// 14. AGENT LOOP
// ============================================================================
function systemPrompt(s, userText, conv, depth = 0) {
  const mems = [...new Map([...searchMemories(userText, 8), ...listMemories().slice(-6)].map((m) => [m.id, m])).values()];
  const proj = projectMemory();
  const todos = (conv.todos || []).length ? `\n## Current task list\n${conv.todos.map((t) => `- [${t.status}] ${t.content}`).join('\n')}\n` : '';
  const modeNote = {
    plan: 'You are in PLAN MODE: read-only tools only. Investigate, then present a numbered plan and stop. Do not edit.',
    ask: 'Edits and shell commands need user approval, so batch your edits to minimise prompts.',
    acceptEdits: 'File edits are auto-approved; shell commands still need approval.',
    yolo: 'Everything is auto-approved. You decide and execute without asking. Move fast but verify with tests.',
  }[s.mode] || '';
  return `You are Power Agent, an autonomous senior software engineer operating directly on the user's machine (workspace: ${WORKSPACE}).
${depth ? 'You are a SUBAGENT: do the delegated slice and report back.' : ''}
Now: ${new Date().toString()}

## Operating rules
1. Never guess about the codebase. Orient with repo_map, glob and grep before editing. Read a file before you patch it.
2. Change code with apply_patch (exact SEARCH/REPLACE). One logical change per call. Keep SEARCH blocks small but unique.
3. Multi-step work: call todo_write first, keep exactly one item in_progress, mark items done as you finish. Update the list when the plan changes.
4. Verify: after edits run the relevant test/build/lint with run_tests or run_command. Fix what breaks. Don't claim success you didn't observe.
5. Delegate broad read-only sweeps to spawn_agent (e.g. "find all callers of X across the repo") instead of reading 20 files yourself.
6. Prefer parallel tool calls when independent. Keep going until the job is actually done — don't stop to ask permission for things you can verify yourself.
7. Cite URLs you used. Answer in the user's language. Markdown. No filler.
8. Files are downloadable at /files/<path>. Show generated images with markdown.
9. When you learn something durable about the user or project, call remember. Never store secrets.
10. Outside plan mode you are fully autonomous. Never ask the user for permission or confirmation to read, edit, run, install, build or test anything. Decide, act, verify. Only stop to ask when you are truly blocked (missing credentials, or an ambiguity that changes the outcome); otherwise make a sensible assumption, state it in one line, and continue.
11. No step or token limit is imposed on you. Work until the whole task is complete and verified: explore, plan, implement, run, fix, re-run. A failing command or tool is information, not a stopping point: diagnose, try another approach, repeat.
12. Install missing dependencies yourself, start dev servers with background=true and test them, and use screenshot to check UI work.
13. For big jobs, split the work into independent slices and call spawn_agent several times in the SAME turn so they run in parallel. Then integrate and verify the combined result.
14. Never stop at suggestions like "you could do X". Do X. End with a short report: what changed, how you verified it, what is left.
15. Giving up is not an option. If something is missing (a browser, ffmpeg, a library, a CLI), install it: call env_info, then install_package (it picks apt/pkg/brew/dnf/pip/npm and has a multi-route headless Chromium setup). If one route fails, try the next: another package, pip vs npm, a pure Node/Python implementation, a prebuilt binary via http_request. Never answer "this device cannot do X" and never hand the work back to the user as manual steps. Deliver exactly what was asked (a real MP4 file, not an HTML substitute) and verify it exists and is valid (ls -l, ffprobe).
16. Everything under the workspace is served live. A .html file runs as a real page (scripts, CDN libraries, localStorage all work) when opened from /files/<path>; .mp4/.png/.pdf open inline. Always give the user the /files/<path> of what you made.
17. You can extend yourself: define_tool creates a persistent custom tool for a missing capability, and mcp_add connects an MCP server (browser automation, databases, ...). Their tools appear on the next step.
18. Use the think tool for long reasoning before complex or risky steps. Reasoning is free and unlimited.

Machine you are running on: ${envLine()}
${modeNote ? `\n## Mode\n${modeNote}\n` : ''}${todos}${proj ? `\n## Project instructions\n${proj}\n` : ''}${s.persona ? `\n## User's custom instructions\n${s.persona}\n` : ''}
## What you remember about the user
${mems.length ? mems.map((m) => `- ${m.text}`).join('\n') : '(nothing yet)'}`;
}

function trimHistory(msgs, budgetTokens) {
  const out = [];
  let used = 0;
  for (let i = msgs.length - 1; i >= 0; i--) {
    const m = msgs[i];
    const size = estTokens(m.content || '') + estTokens(JSON.stringify(m.tool_calls || ''));
    if (used + size > budgetTokens && out.length > 6) break;
    used += size;
    out.unshift(m);
  }
  while (out.length && out[0].role !== 'user') out.shift();
  return out.map((m) => (m.role === 'tool' && m.content.length > 20000 ? { ...m, content: `${m.content.slice(0, 20000)}\n...[truncated]` } : m));
}

async function compact(conv, settings, llm, signal) {
  const head = conv.messages.slice(0, Math.max(0, conv.messages.length - 8));
  if (!head.length) return false;
  const transcript = head.map((m) => `${m.role.toUpperCase()}: ${clip(m.content || JSON.stringify(m.tool_calls || ''), 1500)}`).join('\n');
  const res = await llm.post('/chat/completions', {
    model: settings.model, temperature: 0.2,
    messages: [{ role: 'system', content: 'Summarize this coding session for a continuation. Keep: goal, decisions, files touched, current state, open problems, exact next step. Terse bullets. No preamble.' }, { role: 'user', content: clip(transcript, 40000) }],
  }, signal);
  const summary = res.choices?.[0]?.message?.content || '';
  conv.summary = clip(`${conv.summary}\n${summary}`.trim(), 8000);
  conv.messages = conv.messages.slice(-8);
  saveConversation(conv);
  return true;
}

const GIVE_UP = /\b(cannot|can't|can not|unable to|isn't possible|is not possible|not possible|doesn't support|does not support|no way to|two options|alternatively|workaround)\b|\byou (?:can|could|will need to|need to|have to|should)\b[^.\n]{0,80}\b(?:instead|manually|yourself|on a desktop|copy the)/i;
async function pool(items, n, fn) {
  let i = 0;
  await Promise.all(Array.from({ length: Math.min(Math.max(1, n), items.length) }, async () => {
    while (i < items.length) await fn(items[i++]);
  }));
}

async function runAgent(conv, userText, emit, signal, opts = {}) {
  let settings = opts.settings || getSettings();
  const llm = opts.llm || llmClient(settings);
  const approve = opts.approve || (async () => true);
  if (!conv.messages.length && !conv.title.startsWith('/')) conv.title = userText.replace(/\s+/g, ' ').slice(0, 60);
  conv.messages.push({ role: 'user', content: userText });
  saveConversation(conv);

  const ctx = { conv, convId: conv.id, settings, llm, signal, emit, approve, depth: 0 };
  const maxSteps = opts.maxSteps ?? settings.maxSteps;
  const limit = maxSteps > 0 ? maxSteps : Infinity;          // 0 = unlimited
  const baseModel = settings.model;
  const fallbacks = (settings.fallbackModels || []).filter((m) => m && m !== baseModel);
  let budget = learnedBudget.get(baseModel) || effBudget(settings); // adapts to the real model limit

  const build = (extra = [], withTools = true) => {
    const history = trimHistory(conv.messages, Math.max(4000, budget - 12000));
    const sys = systemPrompt(settings, userText, conv, opts.depth || 0) + (conv.summary ? `\n\n## Earlier in this session (summary)\n${conv.summary}` : '');
    return {
      model: settings.model || 'auto',
      temperature: settings.temperature,
      messages: [{ role: 'system', content: sys }, ...history, ...extra],
      ...(withTools ? { tools: toolSchemas(settings, opts.toolAllow), tool_choice: 'auto' } : {}),
    };
  };

  let useTools = true, carry = '', extra = [], contCount = 0, overflowRetries = 0, transientTries = 0, nudges = 0, fbIdx = 0, stalled = false, gaveUp = false, toolCount = 0;
  const recent = [];
  for (let step = 1; step <= limit; step++) {
    if (signal.aborted) throw new Error('Stopped.');
    if (estTokens(JSON.stringify(conv.messages)) > budget) {
      emit('status', { text: 'Compacting context...' });
      try { await compact(conv, settings, llm, signal); } catch { /* non-fatal */ }
    }
    emit('status', { text: step === 1 ? 'Thinking...' : `Step ${step}${limit === Infinity ? '' : `/${limit}`}` });

    const acc = new Map();
    let content = '', finish = '';
    try {
      await llm.stream(build(extra, useTools), signal, (kind, payload) => {
        if (kind === 'finish') finish = payload;
        else if (kind === 'content') { content += payload; emit('delta', { text: payload }); }
        else {
          const idx = payload.index ?? 0;
          const cur = acc.get(idx) || { id: '', name: '', args: '' };
          if (payload.id) cur.id = payload.id;
          if (payload.function?.name) cur.name += payload.function.name;
          if (payload.function?.arguments) cur.args += payload.function.arguments;
          acc.set(idx, cur);
        }
      });
    } catch (e) {
      if (signal.aborted) throw e;
      // 1) the model told us its real context limit: shrink, compact, retry
      if (isOverflow(e) && overflowRetries < 6) {
        overflowRetries++;
        budget = Math.max(6000, Math.floor(Math.min(budget, estTokens(JSON.stringify(conv.messages))) * 0.6));
        learnedBudget.set(baseModel, budget);
        emit('status', { text: 'Context too large for this model, compacting...' });
        try { await compact(conv, settings, llm, signal); } catch { /* non-fatal */ }
        step--; continue;
      }
      // 2) model has no tool support
      if (useTools && e.status && e.status < 500 && /tool|function/i.test(e.message)) {
        useTools = false; step--; emit('status', { text: 'Model lacks tool support, retrying as plain chat...' }); continue;
      }
      // 3) rate limit / provider down / network: back off, then fail over, never just die
      if (!content && !acc.size && isTransient(e)) {
        if (transientTries < 7) {
          transientTries++;
          const wait = Math.min(30000, 1000 * 2 ** transientTries);
          emit('status', { text: `Provider busy (${e.status || 'network'}), retrying in ${Math.round(wait / 1000)}s...` });
          await sleep(wait, signal); step--; continue;
        }
        const fb = fallbacks[fbIdx++];
        if (fb) {
          emit('status', { text: `Failing over to ${fb}...` });
          settings = { ...settings, model: fb }; ctx.settings = settings;
          budget = learnedBudget.get(fb) || effBudget(settings); transientTries = 0;
          step--; continue;
        }
      }
      throw e;
    }
    transientTries = 0; overflowRetries = 0;

    const calls = [...acc.entries()].filter(([, c]) => c.name).sort((a, b) => a[0] - b[0]).map(([i, c], n) => ({
      id: c.id || `call_${step}_${n}`, type: 'function', function: { name: c.name, arguments: c.args || '{}' },
    }));
    const full = carry + content;

    if (!calls.length) {
      // reply was cut off by the provider's output cap: keep going where it stopped
      if (finish === 'length' && contCount < 25) {
        contCount++; carry = full;
        extra = [{ role: 'assistant', content: full }, { role: 'user', content: 'Continue exactly where you stopped. Do not repeat anything.' }];
        emit('status', { text: 'Continuing...' }); step--; continue;
      }
      carry = ''; extra = []; contCount = 0;
      // unfinished todo list = not done yet: keep working
      const open = (conv.todos || []).filter((t) => t.status !== 'done');
      if (settings.mode !== 'plan' && open.length && nudges < 3 && !opts.depth) {
        nudges++;
        if (full.trim()) emit('thought', { content: full.trim() });
        conv.messages.push({ role: 'assistant', content: full.trim() || '(continuing)' });
        conv.messages.push({ role: 'user', content: `[auto] Your task list still has unfinished items:\n${open.map((t) => `- [${t.status}] ${t.content}`).join('\n')}\nKeep working until each one is done and verified. If an item is no longer needed, mark it done with todo_write, then give the final answer.` });
        saveConversation(conv); continue;
      }
      if (!gaveUp && settings.mode !== 'plan' && !opts.depth && toolCount >= 2 && GIVE_UP.test(full)) {
        gaveUp = true;
        emit('thought', { content: full.trim() });
        conv.messages.push({ role: 'assistant', content: full.trim() });
        conv.messages.push({ role: 'user', content: '[auto] Your answer says something cannot be done, or hands work back to me. Not acceptable. You have a full machine: shell, package managers, network, the ability to install software and write your own tools. Run env_info, install whatever is missing with install_package, try alternative routes, build it yourself, and deliver exactly what was asked. Only if you truly exhausted every route, list precisely what you tried and the exact errors.' });
        saveConversation(conv); continue;
      }
      const final = full.trim() || '(empty response)';
      conv.messages.push({ role: 'assistant', content: final });
      saveConversation(conv);
      emit('final', { content: final });
      return;
    }

    carry = ''; extra = []; contCount = 0;
    toolCount += calls.length;
    conv.messages.push({ role: 'assistant', content: full.trim(), tool_calls: calls });
    if (full.trim()) emit('thought', { content: full.trim() });

    const tools = allTools(settings);
    const results = new Map();
    const queue = calls.map((c) => ({ c, tool: tools[c.function.name] }));
    const runOne = async ({ c }) => {
      emit('tool_start', { id: c.id, name: c.function.name, args: c.function.arguments });
      const out = await runTool(c.function.name, c.function.arguments, ctx);
      results.set(c.id, out);
      emit('tool_end', { id: c.id, output: out.slice(0, 4000) });
    };
    const isPar = (q) => q.tool?.readOnly || q.tool?.parallel;
    await pool(queue.filter(isPar), settings.maxParallel, runOne);
    for (const q of queue.filter((x) => !isPar(x))) { if (signal.aborted) break; await runOne(q); }

    for (const c of calls) conv.messages.push({ role: 'tool', tool_call_id: c.id, content: results.get(c.id) ?? 'Error: not executed.' });

    // loop detection: same calls + same results again and again = stuck, not progress
    const sig = crypto.createHash('sha1').update(calls.map((c) => `${c.function.name}${c.function.arguments}${results.get(c.id) ?? ''}`).join('\u0001')).digest('hex');
    recent.push(sig); if (recent.length > 10) recent.shift();
    const reps = recent.filter((s) => s === sig).length;
    if (reps >= 8) { stalled = true; saveConversation(conv); break; }
    if (reps === 3 || reps === 5) conv.messages.push({ role: 'user', content: '[auto] You made the identical tool call with identical results several times. You are in a loop. Change approach: re-read the error, question your assumptions, try a different tool or strategy, or explain what is blocking you.' });
    saveConversation(conv);
  }

  const res = await withRetry(() => llm.post('/chat/completions', build([{ role: 'user', content: stalled
    ? 'You are stuck repeating the same actions. Stop and give your best final answer: what you did, what is blocking you, and what you recommend next.'
    : 'Step limit reached. Give your best final answer now.' }], false), signal), signal);
  const final = res.choices?.[0]?.message?.content || '(no answer)';
  conv.messages.push({ role: 'assistant', content: final });
  saveConversation(conv);
  emit('final', { content: final });
}

// ============================================================================
// 15. SLASH COMMANDS
// ============================================================================
const COMMANDS = {
  '/help': async (_a, { emit }) => emit('final', { content: 'Commands: /plan /act /init /compact /undo /review /test /todos /memory /model <id> /clear\nMode chips: plan · ask · acceptEdits · yolo (auto-allow, default). /act = back to auto-allow.' }),
  '/init': async (_a, ctx) => {
    const map = repoMap(12000);
    const res = await ctx.llm.post('/chat/completions', { model: ctx.settings.model, temperature: 0.2, messages: [{ role: 'user', content: `Write a POWERAGENT.md onboarding doc for this repo. Sections: purpose, stack, layout, how to run, how to test, conventions, gotchas. Be concrete, <=60 lines.\n\n${map}` }] }, ctx.signal);
    const text = res.choices?.[0]?.message?.content || '';
    const f = wsPath('POWERAGENT.md');
    checkpoint(ctx.convId, 'init', [f]);
    fs.writeFileSync(f, text);
    mapCache.at = 0;
    ctx.emit('final', { content: `Wrote POWERAGENT.md (${text.split('\n').length} lines). It is now injected into every system prompt.` });
  },
  '/compact': async (_a, ctx) => { await compact(ctx.conv, ctx.settings, ctx.llm, ctx.signal); ctx.emit('final', { content: 'Context compacted.' }); },
  '/undo': async (_a, ctx) => {
    const list = listCheckpoints(ctx.convId);
    if (!list.length) return ctx.emit('final', { content: 'Nothing to undo.' });
    const files = restoreCheckpoint(ctx.convId, list[0].id);
    mapCache.at = 0;
    ctx.emit('final', { content: `Reverted ${files.length} file(s) from checkpoint "${list[0].label}".` });
  },
  '/review': async (_a, ctx) => {
    const out = await shellRun('git diff HEAD', { cwd: WORKSPACE, timeout: 60000 });
    const res = await ctx.llm.post('/chat/completions', { model: ctx.settings.model, temperature: 0.2, messages: [{ role: 'user', content: `Review this diff. List real bugs, regressions and missing tests as bullets, most severe first. Skip style nits.\n\n${clip(out, 30000)}` }] }, ctx.signal);
    ctx.emit('final', { content: res.choices?.[0]?.message?.content || '(no review)' });
  },
  '/test': async (_a, ctx) => ctx.emit('final', { content: await TOOLS.run_tests.run({}, ctx) }),
  '/todos': async (_a, ctx) => ctx.emit('final', { content: (ctx.conv.todos || []).map((t) => `[${t.status}] ${t.content}`).join('\n') || 'No todos.' }),
  '/memory': async (_a, ctx) => ctx.emit('final', { content: listMemories().map((m) => `[${m.id}] ${m.text}`).join('\n') || 'Empty.' }),
  '/clear': async (_a, ctx) => { ctx.conv.messages = []; ctx.conv.summary = ''; saveConversation(ctx.conv); ctx.emit('final', { content: 'History cleared.' }); },
};

// ============================================================================
// 16. HTTP SERVER
// ============================================================================
// sessions survive restarts (free hosts sleep and wake all the time); only a hash of the cookie token is stored
const SESS_FILE = path.join(DATA, 'sessions.json');
const sessions = new Map(Object.entries(readJson(SESS_FILE, {})).filter(([, t]) => t > Date.now()));
const sid = (t) => crypto.createHash('sha256').update(String(t)).digest('hex');
const persistSessions = () => { try { for (const [k, t] of sessions) if (t <= Date.now()) sessions.delete(k); fs.writeFileSync(SESS_FILE, JSON.stringify(Object.fromEntries(sessions)), { mode: 0o600 }); } catch { /* ignore */ } };
const running = new Map(), attempts = new Map(), pendingApprovals = new Map();
const MIME = {
  '.html': 'text/html; charset=utf-8', '.htm': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8', '.mjs': 'text/javascript; charset=utf-8',
  '.css': 'text/css; charset=utf-8', '.json': 'application/json', '.map': 'application/json', '.txt': 'text/plain; charset=utf-8', '.md': 'text/plain; charset=utf-8',
  '.csv': 'text/csv', '.py': 'text/plain; charset=utf-8', '.ts': 'text/plain; charset=utf-8', '.jsx': 'text/plain; charset=utf-8', '.tsx': 'text/plain; charset=utf-8', '.xml': 'application/xml',
  '.png': 'image/png', '.jpg': 'image/jpeg', '.jpeg': 'image/jpeg', '.gif': 'image/gif', '.webp': 'image/webp', '.avif': 'image/avif', '.svg': 'image/svg+xml', '.ico': 'image/x-icon', '.bmp': 'image/bmp',
  '.mp4': 'video/mp4', '.m4v': 'video/mp4', '.webm': 'video/webm', '.mov': 'video/quicktime', '.mkv': 'video/x-matroska',
  '.mp3': 'audio/mpeg', '.wav': 'audio/wav', '.ogg': 'audio/ogg', '.m4a': 'audio/mp4', '.flac': 'audio/flac',
  '.woff': 'font/woff', '.woff2': 'font/woff2', '.ttf': 'font/ttf', '.otf': 'font/otf', '.wasm': 'application/wasm',
  '.glb': 'model/gltf-binary', '.gltf': 'model/gltf+json', '.pdf': 'application/pdf', '.zip': 'application/zip',
};
// file server with Range support (video seeking) used by /files and the preview server
function serveFile(req, res, f, headers = {}) {
  const st = fs.statSync(f);
  const base = { 'content-type': MIME[path.extname(f).toLowerCase()] || 'application/octet-stream', 'accept-ranges': 'bytes', 'x-content-type-options': 'nosniff', 'cache-control': 'no-store', ...headers };
  const m = /^bytes=(\d*)-(\d*)$/.exec(req.headers.range || '');
  if (m && (m[1] || m[2])) {
    const start = m[1] ? Number(m[1]) : Math.max(0, st.size - Number(m[2]));
    const end = Math.min(m[1] && m[2] ? Number(m[2]) : st.size - 1, st.size - 1);
    if (start > end || start >= st.size) { res.writeHead(416, { 'content-range': `bytes */${st.size}` }); return res.end(); }
    res.writeHead(206, { ...base, 'content-range': `bytes ${start}-${end}/${st.size}`, 'content-length': end - start + 1 });
    return fs.createReadStream(f, { start, end }).pipe(res);
  }
  res.writeHead(200, { ...base, 'content-length': st.size });
  if (req.method === 'HEAD') return res.end();
  fs.createReadStream(f).pipe(res);
}
// Live preview: generated HTML/apps run on their OWN origin (port previewPort), addressed by a secret path token.
// Different origin = scripts, CDN libs, localStorage all work, and the page can never reach the agent API with your cookie.
const PREVIEW_TOKEN = crypto.randomBytes(18).toString('base64url');
// inline mode = same port as the agent (hosting platforms expose ONE port). The page gets an opaque origin through
// CSP sandbox (no allow-same-origin): scripts and CDN libs run, but it can never use your login cookie or the API.
const PREVIEW_SANDBOX = 'sandbox allow-scripts allow-forms allow-popups allow-modals allow-pointer-lock allow-downloads';
const previewUrl = (req, rel) => {
  const tail = `run/${PREVIEW_TOKEN}/${rel.split('/').map(encodeURIComponent).join('/')}`;
  if (CFG.previewMode === 'inline') return `/${tail}`;
  const host = String(req.headers.host || '127.0.0.1').replace(/:\d+$/, '').replace(/^\[|\]$/g, '');
  return `http://${host.includes(':') ? `[${host}]` : host}:${CFG.previewPort}/${tail}`;
};
function servePreview(req, res, inline) {
  try {
    const u = new URL(req.url, 'http://x'), m = u.pathname.match(/^\/run\/([^/]+)\/?(.*)$/);
    if (!m || m[1] !== PREVIEW_TOKEN) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end('Not found'); }
    let f = wsPath(decodeURIComponent(m[2]) || '.');
    if (exists(f) && fs.statSync(f).isDirectory()) f = path.join(f, 'index.html');
    if (!exists(f) || !fs.statSync(f).isFile()) { res.writeHead(404, { 'content-type': 'text/plain' }); return res.end(`Not found: ${decodeURIComponent(m[2])}`); }
    serveFile(req, res, f, inline ? { 'content-security-policy': PREVIEW_SANDBOX, 'access-control-allow-origin': '*' } : {});
  } catch (e) { res.writeHead(400, { 'content-type': 'text/plain' }); res.end(e.message); }
}
function startPreviewServer() {
  if (CFG.previewMode === 'off') return Promise.resolve(null);
  if (CFG.previewMode === 'inline') { previewUp = true; return Promise.resolve(null); }
  const server = http.createServer((req, res) => servePreview(req, res, false));
  return new Promise((resolve) => {
    server.once('error', (e) => { warn(`Preview server could not start on :${CFG.previewPort} (${e.code || e.message}). Falling back to inline preview.`); CFG.previewMode = 'inline'; previewUp = true; resolve(null); });
    server.listen(CFG.previewPort, CFG.agentHost, () => { previewUp = true; resolve(server); });
  });
}
const send = (res, code, data, headers = {}) => {
  const isObj = typeof data === 'object' && !Buffer.isBuffer(data);
  res.writeHead(code, { 'content-type': isObj ? 'application/json' : 'text/plain; charset=utf-8', 'cache-control': 'no-store', ...headers });
  res.end(isObj ? JSON.stringify(data) : data);
};
async function body(req) {
  const chunks = []; let n = 0;
  for await (const c of req) { if ((n += c.length) > 4e6) throw new Error('Body too large'); chunks.push(c); }
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw ? JSON.parse(raw) : {};
}
const getCookie = (req, k) => (req.headers.cookie || '').split(';').map((s) => s.trim().split('=')).find(([n]) => n === k)?.[1];
const authed = (req) => { const t = getCookie(req, 'pa_session'); return !!t && (sessions.get(sid(t)) || 0) > Date.now(); };
// behind a reverse proxy (Render, Railway, Fly, Cloudflare, nginx...) the real client IP and protocol come in headers
const clientIp = (req) => {
  const sock = req.socket.remoteAddress || '';
  if (!CFG.trustProxyHops) return sock;
  const xs = String(req.headers['x-forwarded-for'] || '').split(',').map((s) => s.trim()).filter(Boolean);
  return xs[xs.length - CFG.trustProxyHops] || sock;
};
const isHttps = (req) => !!req.socket.encrypted || String(req.headers['x-forwarded-proto'] || '').split(',')[0].trim() === 'https';
const cookieAttrs = (req) => `HttpOnly; SameSite=Strict; Path=/${isHttps(req) ? '; Secure' : ''}`;
const hostOf = (s) => { try { return new URL(String(s).trim()).host; } catch { return ''; } };
const extraHosts = [CFG.publicUrl, ...String(ENV.ALLOWED_ORIGINS || '').split(',')].map(hostOf).filter(Boolean);
const allowedHosts = (req) => [req.headers.host, ...String(req.headers['x-forwarded-host'] || '').split(',').map((s) => s.trim()), ...extraHosts].filter(Boolean);
const dashUrl = (s) => (USE_FREELLM && !s.baseUrl ? FREELLM_URL : '');
const safeEq = (a, b) => { const x = Buffer.from(String(a)), y = Buffer.from(String(b)); return x.length === y.length && crypto.timingSafeEqual(x, y); };
const mask = (k) => (k ? `${k.slice(0, 12)}...${k.slice(-4)}` : '');

function startAgentServer() {
  const server = http.createServer(async (req, res) => {
    const p = new URL(req.url, 'http://x').pathname;
    try {
      if (req.method !== 'GET' && req.method !== 'HEAD' && req.headers.origin) { // CSRF guard: other origins (incl. other local ports) may not call the API
        let ok = false;
        try { ok = allowedHosts(req).includes(new URL(req.headers.origin).host); } catch { /* bad origin */ }
        if (!ok) return send(res, 403, { error: 'Cross-origin request blocked' });
      }
      if (p === '/health') return send(res, 200, { ok: true, llm: bootState.phase, message: bootState.message });
      if (CFG.previewMode === 'inline' && p.startsWith('/run/') && (req.method === 'GET' || req.method === 'HEAD')) return servePreview(req, res, true); // token-protected, before login
      if (req.method === 'GET' && (p === '/' || p === '/index.html')) return send(res, 200, PAGE, { 'content-type': 'text/html; charset=utf-8' });
      if (p === '/api/me') return send(res, 200, { authed: authed(req) });
      if (p === '/api/login' && req.method === 'POST') {
        const ip = clientIp(req), a = attempts.get(ip) || { n: 0, t: Date.now() };
        if (Date.now() - a.t > 6e5) Object.assign(a, { n: 0, t: Date.now() });
        if (a.n >= 10) return send(res, 429, { error: 'Too many attempts. Wait 10 minutes.' });
        const { password } = await body(req);
        if (!safeEq(password || '', CFG.password)) { a.n++; attempts.set(ip, a); return send(res, 401, { error: 'Wrong password' }); }
        const tok = crypto.randomBytes(24).toString('hex');
        sessions.set(sid(tok), Date.now() + 30 * 864e5); persistSessions();
        return send(res, 200, { ok: true }, { 'set-cookie': `pa_session=${tok}; ${cookieAttrs(req)}; Max-Age=2592000` });
      }
      if (!authed(req)) return send(res, 401, { error: 'Login required' });

      if (p === '/api/logout') { sessions.delete(sid(getCookie(req, 'pa_session'))); persistSessions(); return send(res, 200, { ok: true }, { 'set-cookie': `pa_session=; ${cookieAttrs(req)}; Max-Age=0` }); }

      if (p === '/api/preview' && req.method === 'GET') {
        const f = wsPath(new URL(req.url, 'http://x').searchParams.get('path') || '');
        if (!exists(f)) return send(res, 404, { error: 'Not found' });
        return send(res, 200, { url: previewUrl(req, relPath(f)), up: previewUp });
      }
      if (p.startsWith('/files/') && (req.method === 'GET' || req.method === 'HEAD')) {
        const f = wsPath(decodeURIComponent(p.slice(7)));
        if (!exists(f) || !fs.statSync(f).isFile()) return send(res, 404, 'Not found');
        const q = new URL(req.url, 'http://x').searchParams, dl = q.get('download');
        if (!dl && !q.get('raw') && previewUp && /\.html?$/i.test(f)) { res.writeHead(302, { location: previewUrl(req, relPath(f)) }); return res.end(); } // HTML runs in the preview origin
        return serveFile(req, res, f, {
          'content-security-policy': "default-src 'none'; img-src 'self' data:; media-src 'self'; style-src 'unsafe-inline'",
          ...(dl ? { 'content-disposition': `attachment; filename="${path.basename(f).replace(/"/g, '')}"` } : {}),
        });
      }

      if (p === '/api/settings' && req.method === 'GET') { const s = getSettings(); return send(res, 200, { ...s, apiKey: mask(s.apiKey), hasKey: !!s.apiKey, dashboardUrl: dashUrl(s), hosted: HOSTED, platform: PLATFORM, llm: bootState, workspace: WORKSPACE, autoBudget: AUTO_BUDGET }); }
      if (p === '/api/settings' && req.method === 'POST') {
        const b = await body(req);
        if (!b.apiKey || b.apiKey.includes('...')) delete b.apiKey;
        const s = saveSettings(b);
        return send(res, 200, { ...s, apiKey: mask(s.apiKey), hasKey: !!s.apiKey, dashboardUrl: dashUrl(s) });
      }
      if (p === '/api/models') {
        try { const j = await llmClient(getSettings()).get('/models'); return send(res, 200, { models: (j.data || []).map((m) => m.id) }); }
        catch (e) { return send(res, 200, { models: [], error: e.message }); }
      }
      if (p === '/api/tree') { try { return send(res, 200, await TOOLS.tree.run({ path: '.', depth: 2 })); } catch (e) { return send(res, 200, e.message); } }

      // ---------------- uploads (composer attachments) ----------------
      if (p === '/api/upload' && req.method === 'POST') {
        const q = new URL(req.url, 'http://x').searchParams;
        const dir = wsPath(q.get('dir') || 'uploads');
        const name = path.basename(String(q.get('name') || `paste_${newId(4)}.bin`)).replace(/[^\w.\- ]/g, '_');
        const f = path.join(dir, name);
        const chunks = []; let n = 0;
        for await (const c of req) { if ((n += c.length) > 40e6) return send(res, 413, { error: 'File over 40 MB' }); chunks.push(c); }
        fs.mkdirSync(dir, { recursive: true });
        fs.writeFileSync(f, Buffer.concat(chunks));
        mapCache.at = 0;
        return send(res, 200, { path: relPath(f), size: n, url: fileUrl(f) });
      }

      // ---------------- file manager ----------------
      if (p.startsWith('/api/fs')) {
        const q = new URL(req.url, 'http://x').searchParams;
        const op = p.split('/')[3] || '';
        const rp = q.get('path') || '.';
        if (op === 'list') {
          const dir = wsPath(rp);
          if (!fs.existsSync(dir) || !fs.statSync(dir).isDirectory()) return send(res, 404, { error: 'Not a directory' });
          const entries = fs.readdirSync(dir, { withFileTypes: true })
            .filter((e) => e.name !== '.git')
            .map((e) => { const st = fs.statSync(path.join(dir, e.name)); return { name: e.name, dir: e.isDirectory(), size: e.isDirectory() ? 0 : st.size }; })
            .sort((a, b) => (a.dir === b.dir ? a.name.localeCompare(b.name) : a.dir ? -1 : 1));
          return send(res, 200, { path: relPath(dir) || '.', entries });
        }
        if (op === 'read') {
          const f = wsPath(rp);
          if (!fs.existsSync(f) || !fs.statSync(f).isFile()) return send(res, 404, { error: 'Not found' });
          const buf = fs.readFileSync(f);
          const bin = isBinary(buf);
          return send(res, 200, { path: relPath(f), size: buf.length, binary: bin, image: IMG_EXT.has(path.extname(f).toLowerCase()), content: bin ? '' : clip(buf.toString('utf8'), 500000) });
        }
        if (op === 'write' && req.method === 'POST') {
          const { path: target, content = '' } = await body(req);
          const f = wsPath(target);
          checkpoint('files', `ui save ${target}`, [f]);
          fs.mkdirSync(path.dirname(f), { recursive: true });
          fs.writeFileSync(f, content);
          mapCache.at = 0;
          return send(res, 200, { ok: true, size: Buffer.byteLength(content) });
        }
        if (op === 'mkdir' && req.method === 'POST') {
          const { path: target } = await body(req);
          fs.mkdirSync(wsPath(target), { recursive: true });
          mapCache.at = 0;
          return send(res, 200, { ok: true });
        }
        if (op === 'rename' && req.method === 'POST') {
          const { from, to } = await body(req);
          const a = wsPath(from), b = wsPath(to);
          if (!fs.existsSync(a)) return send(res, 404, { error: 'Not found' });
          if (fs.existsSync(b)) return send(res, 409, { error: 'Target already exists' });
          checkpoint('files', `ui rename ${from}`, [a, b]);
          fs.mkdirSync(path.dirname(b), { recursive: true });
          fs.renameSync(a, b);
          mapCache.at = 0;
          return send(res, 200, { ok: true });
        }
        if (op === 'delete' && req.method === 'POST') {
          const { path: target } = await body(req);
          const f = wsPath(target);
          if (!fs.existsSync(f)) return send(res, 404, { error: 'Not found' });
          checkpoint('files', `ui delete ${target}`, fs.statSync(f).isDirectory() ? [...walk(f)] : [f]);
          fs.rmSync(f, { recursive: true, force: true });
          mapCache.at = 0;
          return send(res, 200, { ok: true });
        }
        if (op === 'upload' && req.method === 'POST') {
          const dir = wsPath(rp);
          const name = path.basename(String(q.get('name') || 'upload.bin')).replace(/[^\w.\- ]/g, '_');
          const f = path.join(dir, name);
          const chunks = []; let n = 0;
          for await (const c of req) { if ((n += c.length) > 30e6) return send(res, 413, { error: 'File over 30 MB' }); chunks.push(c); }
          checkpoint('files', `ui upload ${name}`, [f]);
          fs.writeFileSync(f, Buffer.concat(chunks));
          mapCache.at = 0;
          return send(res, 200, { ok: true, path: relPath(f), size: n });
        }
        return send(res, 404, { error: 'Unknown fs op' });
      }

      if (p === '/api/checkpoints') return send(res, 200, listCheckpoints(new URL(req.url, 'http://x').searchParams.get('conv') || ''));
      if (p === '/api/memory' && req.method === 'GET') return send(res, 200, listMemories().reverse());
      if (p === '/api/memory' && req.method === 'POST') return send(res, 200, addMemory((await body(req)).text));
      if (p.startsWith('/api/memory/') && req.method === 'DELETE') return send(res, 200, { ok: deleteMemory(p.split('/').pop()) });
      if (p === '/api/conversations' && req.method === 'GET') return send(res, 200, listConversations());
      if (p === '/api/conversations' && req.method === 'POST') return send(res, 200, createConversation());

      if (p.startsWith('/api/approve/') && req.method === 'POST') {
        const { allow, always } = await body(req);
        const id = p.split('/').pop();
        const pa = pendingApprovals.get(id);
        if (!pa) return send(res, 404, { error: 'No pending approval' });
        pendingApprovals.delete(id);
        if (always && allow) {
          const s = getSettings();
          if (!s.autoApprove.includes(pa.tool)) saveSettings({ autoApprove: [...s.autoApprove, pa.tool] });
        }
        pa.resolve(!!allow);
        return send(res, 200, { ok: true });
      }

      const m = p.match(/^\/api\/conversations\/([a-f0-9]+)(?:\/(chat|stop|fork))?$/);
      if (m) {
        const conv = getConversation(m[1]);
        if (!conv) return send(res, 404, { error: 'Chat not found' });
        if (!m[2] && req.method === 'GET') return send(res, 200, conv);
        if (!m[2] && req.method === 'DELETE') { running.get(conv.id)?.abort(); fs.rmSync(convFile(conv.id), { force: true }); return send(res, 200, { ok: true }); }
        if (m[2] === 'stop') { running.get(conv.id)?.abort(); return send(res, 200, { ok: true }); }
        if (m[2] === 'fork') {
          const copy = { ...createConversation(`${conv.title} (fork)`), messages: conv.messages.slice(-20), summary: conv.summary };
          saveConversation(copy);
          return send(res, 200, copy);
        }
        if (m[2] === 'chat' && req.method === 'POST') {
          const { message } = await body(req);
          if (!message?.trim()) return send(res, 400, { error: 'Empty message' });
          if (running.has(conv.id)) return send(res, 409, { error: 'Still working on this chat.' });
          res.writeHead(200, { 'content-type': 'text/event-stream', 'cache-control': 'no-store', connection: 'keep-alive', 'x-accel-buffering': 'no' });
          const emit = (ev, d) => { if (!res.writableEnded) res.write(`event: ${ev}\ndata: ${JSON.stringify(d)}\n\n`); };
          const ac = new AbortController(), ping = setInterval(() => !res.writableEnded && res.write(': ping\n\n'), 15000);
          running.set(conv.id, ac);
          const settings = getSettings();
          const approve = (tool, args) => new Promise((resolve) => {
            const id = newId();
            pendingApprovals.set(id, { resolve, tool });
            emit('approval', { id, tool, args });
            setTimeout(() => { if (pendingApprovals.delete(id)) resolve(false); }, 300000);
          });
          const ctx = { conv, convId: conv.id, settings, llm: llmClient(settings), signal: ac.signal, emit, approve, depth: 0 };
          try {
            const text = message.trim();
            if (text.startsWith('/')) {
              const [cmd, ...rest] = text.split(/\s+/);
              if (COMMANDS[cmd]) {
                if (cmd === '/plan' || cmd === '/act') {
                  saveSettings({ mode: cmd === '/plan' ? 'plan' : 'yolo' });
                  emit('final', { content: `Mode: ${cmd === '/plan' ? 'plan (read-only)' : 'yolo (auto-allow)'}` });
                } else if (cmd === '/model') {
                  if (rest[0]) saveSettings({ model: rest[0] });
                  emit('final', { content: `Model: ${getSettings().model}` });
                } else await COMMANDS[cmd](rest.join(' '), ctx);
              } else emit('final', { content: `Unknown command ${cmd}. Try /help` });
            } else {
              await runAgent(conv, text, emit, ac.signal, { settings, llm: ctx.llm, approve });
            }
          } catch (e) { emit('error', { message: ac.signal.aborted ? 'Stopped.' : e.message }); }
          finally { clearInterval(ping); running.delete(conv.id); emit('done', { title: conv.title }); res.end(); }
          return;
        }
      }
      send(res, 404, { error: 'Not found' });
    } catch (e) {
      alog(`error: ${e.message}`);
      if (!res.headersSent) send(res, 500, { error: e.message }); else res.end();
    }
  });
  server.keepAliveTimeout = 65000; server.headersTimeout = 66000; // longer than typical proxy idle timeouts, avoids random 502s
  return new Promise((resolve, reject) => { server.once('error', reject); server.listen(CFG.agentPort, CFG.agentHost, () => resolve(server)); });
}

// ============================================================================
// 17. FRONTEND
// ============================================================================
const LIGHT = '--bg:#faf9f5;--side:#f5f4ed;--elev:#ffffff;--hover:#ece9de;--border:rgba(31,30,29,.15);--text:#141413;--text2:#3d3d3a;--text3:#73726c;--accent:#c6613f;--accent2:#b4553a;--on-accent:#fff;--user:#f0eee6;--code:#f5f4ed;--danger:#b9382f;--ok:#4d8a3a;--shadow:0 8px 30px rgba(20,20,19,.12);color-scheme:light';
const CSS = `
:root{--sans:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,sans-serif;--serif:"Iowan Old Style","Palatino Linotype",Palatino,Georgia,"Source Serif 4",serif;--mono:ui-monospace,SFMono-Regular,"SF Mono",Menlo,Consolas,"Liberation Mono",monospace;
--bg:#262624;--side:#1f1e1d;--elev:#30302e;--hover:#3a3a37;--border:rgba(222,220,209,.14);--text:#faf9f5;--text2:#c2c0b6;--text3:#8f8d84;--accent:#d97757;--accent2:#e68a6b;--on-accent:#fff;--user:#141413;--code:#1f1e1d;--danger:#e5736b;--ok:#7fb069;--shadow:0 8px 32px rgba(0,0,0,.4);color-scheme:dark}
:root[data-theme=light]{${LIGHT}}
@media (prefers-color-scheme:light){:root:not([data-theme]){${LIGHT}}}
*{box-sizing:border-box;-webkit-tap-highlight-color:transparent}
html,body{height:100%;margin:0}
body{background:var(--bg);color:var(--text);font:16px/1.5 var(--sans);-webkit-font-smoothing:antialiased;overflow:hidden}
.hidden{display:none!important}
button{font:inherit;color:inherit;background:none;border:0;cursor:pointer;padding:0}
a{color:inherit}
svg.ic{width:20px;height:20px;flex:none;display:block}
svg.ic.sm{width:15px;height:15px}
code,pre{font-family:var(--mono)}
.muted{color:var(--text3)}
::selection{background:color-mix(in srgb,var(--accent) 35%,transparent)}
*{scrollbar-width:thin;scrollbar-color:var(--hover) transparent}

/* ---------- buttons ---------- */
.btn{display:inline-flex;align-items:center;justify-content:center;gap:8px;height:38px;padding:0 16px;border-radius:10px;border:1px solid var(--border);background:var(--elev);font-weight:500;font-size:14.5px}
.btn:hover{background:var(--hover)}
.btn.sm{height:32px;padding:0 12px;font-size:13.5px;border-radius:8px}
.btn.accent,.btn.primary{background:var(--accent);border-color:transparent;color:var(--on-accent)}
.btn.accent:hover,.btn.primary:hover{background:var(--accent2)}
.btn.ghost{background:transparent;border-color:transparent}
.btn.danger{color:var(--danger)}
.btn.wide{width:100%}
.icon-btn{display:inline-flex;align-items:center;justify-content:center;width:36px;height:36px;border-radius:9px;color:var(--text2);text-decoration:none;flex:none}
.icon-btn:hover{background:var(--hover);color:var(--text)}
.link{background:none;border:0;color:var(--accent);text-decoration:underline;cursor:pointer;padding:0;font:inherit}
.only-mobile{display:none}

/* ---------- login ---------- */
.login{height:100dvh;display:flex;align-items:center;justify-content:center;padding:24px}
.login form{width:min(380px,100%);text-align:center;display:flex;flex-direction:column;gap:14px;align-items:center}
.login .spark-lg svg{width:46px;height:46px;color:var(--accent)}
.login h1{font:400 34px/1.1 var(--serif);margin:0}
.login p{margin:0}
.login input{width:100%;height:48px;border-radius:14px;border:1px solid var(--border);background:var(--elev);color:var(--text);padding:0 16px;font-size:16px;outline:none}
.login input:focus{border-color:var(--accent)}
.login .btn{height:46px;border-radius:12px;font-size:16px}
.err{color:var(--danger);font-size:14px;min-height:1em}

/* ---------- layout ---------- */
.app{display:flex;height:100dvh;overflow:hidden}
.side{width:288px;flex:none;background:var(--side);border-right:1px solid var(--border);display:flex;flex-direction:column;padding:10px 10px calc(10px + env(safe-area-inset-bottom));z-index:50}
.side-top{display:flex;align-items:center;justify-content:space-between;padding:6px 6px 10px}
.brand{display:flex;align-items:center;gap:9px}
.brand .mark svg{width:24px;height:24px;color:var(--accent)}
.wordmark{font:400 21px/1 var(--serif);letter-spacing:-.01em}
.newchat{display:flex;align-items:center;gap:10px;width:100%;height:42px;padding:0 8px;border-radius:10px;font-weight:500;font-size:15px}
.newchat:hover{background:var(--hover)}
.newchat .plus{width:28px;height:28px;border-radius:50%;background:var(--accent);color:var(--on-accent);display:flex;align-items:center;justify-content:center}
.newchat .plus svg{width:17px;height:17px}
.nav{display:flex;flex-direction:column;margin:2px 0 8px}
.nav-item{display:flex;align-items:center;gap:12px;height:38px;padding:0 10px;border-radius:9px;color:var(--text2);font-size:15px;text-align:left}
.nav-item:hover{background:var(--hover);color:var(--text)}
.side-label{padding:10px 12px 6px;font-size:12px;letter-spacing:.04em;color:var(--text3)}
.convs{flex:1;overflow-y:auto;display:flex;flex-direction:column;gap:1px;min-height:0}
.conv{display:flex;align-items:center;border-radius:9px;position:relative}
.conv a{flex:1;min-width:0;padding:8px 10px;font-size:14.5px;color:var(--text2);text-decoration:none;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;display:block}
.conv:hover,.conv.active{background:var(--hover)}
.conv.active a,.conv:hover a{color:var(--text)}
.conv button{width:30px;height:30px;margin-right:4px;border-radius:7px;color:var(--text3);display:flex;align-items:center;justify-content:center;opacity:0}
.conv:hover button,.conv.active button{opacity:1}
.conv button:hover{color:var(--danger);background:var(--bg)}
.conv .badge{display:inline-block;min-width:18px;padding:0 5px;margin-right:6px;border-radius:9px;background:var(--accent);color:var(--on-accent);font-size:11px;text-align:center}
.side-foot{display:flex;align-items:center;gap:2px;padding-top:10px;border-top:1px solid var(--border);margin-top:8px}
.who{display:flex;align-items:center;gap:10px;flex:1;min-width:0}
.who b{display:block;font-size:14px;font-weight:600;line-height:1.2}
.who small{display:block;color:var(--text3);font-size:12px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.who>div{min-width:0}
.avatar{width:32px;height:32px;border-radius:50%;background:var(--hover);display:flex;align-items:center;justify-content:center;color:var(--text2);flex:none}
.avatar svg{width:17px;height:17px}
.scrim{display:none}
.main{flex:1;min-width:0;display:flex;flex-direction:column;position:relative}
.top{height:54px;flex:none;display:flex;align-items:center;gap:6px;padding:0 10px}
.title{flex:1;min-width:0;font-size:15px;font-weight:500;padding:0 6px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.ring-btn{width:36px;height:36px;border-radius:9px;display:flex;align-items:center;justify-content:center}
.ring-btn:hover{background:var(--hover)}
.ring-btn svg{width:22px;height:22px;transform:rotate(-90deg)}
.ring-bg{fill:none;stroke:var(--hover);stroke-width:3.6}
.ring-fg{fill:none;stroke:var(--text3);stroke-width:3.6;stroke-linecap:round;transition:stroke-dasharray .3s}
.ring-fg.warn{stroke:var(--accent)}
.banner{margin:0 12px 8px;padding:10px 14px;border-radius:12px;background:var(--elev);border:1px solid var(--border);font-size:14px}
.msgs{flex:1;overflow-y:auto;padding:8px 16px 28px;min-height:0;scroll-behavior:auto}
.msgs>*{max-width:46rem;margin-left:auto;margin-right:auto}
.to-bottom{position:absolute;left:50%;transform:translateX(-50%);bottom:178px;width:36px;height:36px;border-radius:50%;background:var(--elev);border:1px solid var(--border);box-shadow:var(--shadow);display:flex;align-items:center;justify-content:center;z-index:5}

/* ---------- empty state ---------- */
.empty{min-height:100%;display:flex;flex-direction:column;align-items:center;justify-content:center;text-align:center;padding:24px 0 40px}
.hello{display:flex;align-items:center;gap:14px;margin-bottom:6px}
.hello svg{width:38px;height:38px;color:var(--accent)}
.hello h2{font:400 clamp(30px,6vw,42px)/1.15 var(--serif);margin:0;letter-spacing:-.015em}
.empty p{color:var(--text3);margin:0 0 22px;font-size:16px}
.chips{display:flex;flex-wrap:wrap;gap:8px;justify-content:center;max-width:40rem}
.chip{display:inline-flex;align-items:center;gap:8px;height:38px;padding:0 15px;border-radius:19px;border:1px solid var(--border);color:var(--text2);font-size:14.5px;background:transparent}
.chip:hover{background:var(--elev);color:var(--text)}
.chip svg{width:16px;height:16px}

/* ---------- messages ---------- */
.user{display:flex;gap:10px;align-items:flex-start;margin-top:22px}
.user .avatar{width:28px;height:28px;margin-top:3px}
.user .avatar svg{width:15px;height:15px}
.ub{background:var(--user);border-radius:16px;padding:10px 15px;white-space:pre-wrap;word-break:break-word;font-size:15.5px;line-height:1.55;min-width:0}
.auto-note{margin-top:14px;margin-bottom:2px;text-align:center;font-size:12.5px;color:var(--text3)}
.auto-note span{display:inline-block;padding:3px 11px;border-radius:12px;border:1px dashed var(--border)}
.bot{font:17px/1.72 var(--serif);margin-top:20px;word-break:break-word;color:var(--text)}
.bot.interim{color:var(--text2);margin-top:14px}
.bot>*:first-child{margin-top:0}.bot>*:last-child{margin-bottom:0}
.bot p{margin:0 0 .85em}
.bot h2,.bot h3,.bot h4,.bot h5{font-family:var(--serif);font-weight:600;line-height:1.3;margin:1.3em 0 .5em}
.bot h2{font-size:1.35em}.bot h3{font-size:1.18em}.bot h4,.bot h5{font-size:1.05em}
.bot ul,.bot ol{padding-left:1.4em;margin:0 0 .9em}.bot li{margin:.25em 0}.bot li::marker{color:var(--text3)}
.bot a{color:var(--accent);text-decoration:underline;text-underline-offset:3px;text-decoration-color:color-mix(in srgb,var(--accent) 45%,transparent)}
.bot a:hover{text-decoration-color:var(--accent)}
.bot :not(pre)>code{font-size:.84em;background:var(--code);border:1px solid var(--border);padding:.12em .4em;border-radius:6px}
.bot b{font-weight:650}
.bot img,.bot video{max-width:100%;border-radius:12px;border:1px solid var(--border);display:block;margin:10px 0}
.bot video{width:100%;background:#000}
.bot pre{background:var(--code);border:1px solid var(--border);border-radius:12px;margin:0 0 1em;overflow:hidden;font-size:13.5px;line-height:1.55}
.bot pre .ch{display:flex;align-items:center;justify-content:space-between;padding:7px 8px 7px 14px;font:12px var(--sans);color:var(--text3);border-bottom:1px solid var(--border)}
.bot pre .copy{font:12px var(--sans);color:var(--text2);padding:4px 9px;border-radius:7px}
.bot pre .copy:hover{background:var(--hover)}
.bot pre code{display:block;padding:12px 14px;overflow-x:auto;white-space:pre;color:var(--text)}
.bot table{border-collapse:collapse;display:block;overflow-x:auto;margin:0 0 1em;font:14.5px var(--sans)}
.bot th,.bot td{border:1px solid var(--border);padding:7px 12px;text-align:left}
.bot th{background:var(--code)}
.bot blockquote{margin:0 0 .9em;padding-left:14px;border-left:3px solid var(--border);color:var(--text2)}
.macts{display:flex;align-items:center;gap:2px;margin-top:6px;color:var(--text3)}
.macts button{width:30px;height:30px;border-radius:8px;display:flex;align-items:center;justify-content:center}
.macts button:hover{background:var(--hover);color:var(--text)}
.macts svg{width:16px;height:16px}
.macts .sp{margin-left:auto;color:var(--accent);display:flex}
.macts .sp svg{width:18px;height:18px}

/* ---------- steps (tool use) ---------- */
.steps{margin-top:14px;border:1px solid var(--border);border-radius:12px;background:transparent;font-size:14px}
.steps>summary{display:flex;align-items:center;gap:9px;padding:9px 12px;cursor:pointer;list-style:none;color:var(--text2);user-select:none}
.steps>summary::-webkit-details-marker,.tool>summary::-webkit-details-marker{display:none}
.steps>summary .lbl{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.steps>summary .chev,.tool>summary .chev{transition:transform .15s;color:var(--text3);width:15px;height:15px}
.steps[open]>summary .chev,.tool[open]>summary .chev{transform:rotate(90deg)}
.steps.running>summary .lbl{color:var(--text)}
.st{display:flex;width:16px;justify-content:center;color:var(--ok)}
.st svg{width:16px;height:16px}
.steps-body{border-top:1px solid var(--border);padding:4px 6px 6px;display:flex;flex-direction:column}
.tool>summary{display:flex;align-items:center;gap:9px;padding:7px 8px;border-radius:8px;cursor:pointer;list-style:none;font-size:13.5px;color:var(--text2)}
.tool>summary:hover{background:var(--hover)}
.tool .ti{display:flex;width:16px;justify-content:center;color:var(--text3)}
.tool .ti svg{width:15px;height:15px}
.tool.ok .ti{color:var(--ok)}.tool.bad .ti{color:var(--danger)}
.tool .tl{font-weight:600;color:var(--text);white-space:nowrap}
.tool .ta{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-family:var(--mono);font-size:12.5px;color:var(--text3)}
.tool pre{margin:4px 0 6px 25px;padding:10px 12px;background:var(--code);border:1px solid var(--border);border-radius:9px;font-size:12px;line-height:1.5;max-height:340px;overflow:auto;white-space:pre-wrap;word-break:break-word;color:var(--text2)}
.tool pre .add{color:var(--ok)}.tool pre .del{color:var(--danger)}
.spin{width:14px;height:14px;border-radius:50%;border:2px solid var(--hover);border-top-color:var(--accent);animation:rot .8s linear infinite;display:inline-block}
@keyframes rot{to{transform:rotate(360deg)}}
@keyframes pulse{0%,100%{opacity:.45;transform:scale(.92)}50%{opacity:1;transform:scale(1.06)}}
.status{display:flex;align-items:center;gap:10px;margin-top:18px;color:var(--text3);font-size:14.5px}
.status .sp{color:var(--accent);display:flex;animation:pulse 1.6s ease-in-out infinite}
.status .sp svg{width:22px;height:22px;animation:rot 6s linear infinite}
.status .el{margin-left:2px;font-variant-numeric:tabular-nums;opacity:.7}

/* ---------- artifacts ---------- */
.art{display:flex;align-items:center;gap:12px;margin-top:10px;padding:10px 12px;border:1px solid var(--border);border-radius:14px;background:var(--elev);cursor:pointer;width:100%;text-align:left}
.art:hover{border-color:color-mix(in srgb,var(--accent) 55%,var(--border))}
.art .ai{width:40px;height:40px;border-radius:10px;background:var(--hover);display:flex;align-items:center;justify-content:center;color:var(--accent);flex:none}
.art .at{min-width:0;flex:1}.art .at b{display:block;font-size:14.5px;font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.art .at small{color:var(--text3);font-size:12.5px}
.art>svg{color:var(--text3)}
.errbox{margin-top:14px;padding:10px 14px;border:1px solid color-mix(in srgb,var(--danger) 55%,transparent);border-radius:12px;color:var(--danger);font-size:14px;white-space:pre-wrap}
.appr{margin-top:14px;padding:14px;border:1px solid var(--accent);border-radius:14px;background:var(--elev);font-size:14.5px}
.appr code{display:block;margin:8px 0 12px;padding:9px 11px;border-radius:9px;background:var(--code);font-size:12.5px;white-space:pre-wrap;word-break:break-all;max-height:160px;overflow:auto}
.appr .row{display:flex;gap:8px;flex-wrap:wrap}

/* ---------- dock / composer ---------- */
.dock{flex:none;padding:0 12px calc(8px + env(safe-area-inset-bottom));max-width:calc(46rem + 24px);width:100%;margin:0 auto;position:relative}
.fine{margin:8px 0 0;text-align:center;font-size:11.5px;color:var(--text3)}
.todos{margin-bottom:8px;border:1px solid var(--border);border-radius:14px;background:var(--elev);font-size:14px}
.todos summary{display:flex;align-items:center;gap:9px;padding:9px 13px;cursor:pointer;list-style:none}
.todos summary::-webkit-details-marker{display:none}
.todos summary b{font-weight:600}
.todos summary .muted{flex:1}
.todos .chev{transition:transform .15s;width:15px;height:15px;color:var(--text3)}
.todos[open] .chev{transform:rotate(180deg)}
.todos ul{list-style:none;margin:0;padding:2px 13px 11px;max-height:34vh;overflow:auto}
.todos li{display:flex;gap:10px;align-items:flex-start;padding:3px 0;color:var(--text2)}
.todos li .dot{width:16px;height:16px;border-radius:50%;border:1.6px solid var(--text3);margin-top:3px;flex:none;display:flex;align-items:center;justify-content:center}
.todos li.in_progress .dot{border-color:var(--accent);border-top-color:transparent;animation:rot 1s linear infinite}
.todos li.done .dot{background:var(--ok);border-color:var(--ok);color:var(--bg)}
.todos li.done .dot svg{width:11px;height:11px;stroke-width:3}
.todos li.done span:last-child{color:var(--text3);text-decoration:line-through}
.cmds{margin-bottom:8px;border:1px solid var(--border);border-radius:14px;background:var(--elev);box-shadow:var(--shadow);overflow:hidden}
.cmds button{display:flex;gap:12px;width:100%;padding:9px 14px;text-align:left;font-size:14.5px}
.cmds button:hover{background:var(--hover)}
.cmds b{font-family:var(--mono);font-weight:500;color:var(--accent);min-width:78px}
.cmds span{color:var(--text3)}
.composer{background:var(--elev);border:1px solid var(--border);border-radius:24px;padding:12px 12px 10px 14px;box-shadow:0 4px 30px rgba(0,0,0,.18);transition:border-color .15s}
.composer:focus-within{border-color:color-mix(in srgb,var(--text3) 60%,var(--border))}
.composer textarea{display:block;width:100%;border:0;outline:0;resize:none;background:transparent;color:var(--text);font:16px/1.5 var(--sans);padding:2px 2px 8px;max-height:220px;min-height:30px}
.composer textarea::placeholder{color:var(--text3)}
.crow{display:flex;align-items:center;gap:6px}
.crow .grow{flex:1}
.circle-btn{width:34px;height:34px;border-radius:50%;border:1px solid var(--border);display:flex;align-items:center;justify-content:center;color:var(--text2);flex:none}
.circle-btn:hover{background:var(--hover);color:var(--text)}
.chip-btn{display:inline-flex;align-items:center;gap:6px;height:34px;padding:0 11px;border-radius:17px;border:1px solid var(--border);font-size:13.5px;color:var(--text2);max-width:46vw}
.chip-btn:hover{background:var(--hover);color:var(--text)}
.chip-btn svg{width:16px;height:16px}
.chip-btn span{white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.chip-btn[data-mode=yolo]{color:var(--accent);border-color:color-mix(in srgb,var(--accent) 45%,var(--border))}
.chip-btn[data-mode=plan]{color:#6aa8ff;border-color:color-mix(in srgb,#6aa8ff 45%,var(--border))}
.chip-btn.plain{border-color:transparent}
.send{width:36px;height:36px;border-radius:10px;background:var(--accent);color:var(--on-accent);display:flex;align-items:center;justify-content:center;flex:none}
.send:hover{background:var(--accent2)}
.send svg{width:19px;height:19px;stroke-width:2.3}
.send.stop{background:var(--text);color:var(--bg)}
.send:disabled{opacity:.4;cursor:default}
.attach{display:flex;flex-wrap:wrap;gap:6px;padding-bottom:8px}
.pill{display:inline-flex;align-items:center;gap:7px;height:32px;padding:0 6px 0 10px;border-radius:9px;background:var(--hover);font-size:13px;max-width:100%}
.pill svg{width:15px;height:15px;color:var(--text3)}
.pill span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:200px}
.pill button{width:22px;height:22px;border-radius:6px;display:flex;align-items:center;justify-content:center;color:var(--text3)}
.pill button:hover{background:var(--bg);color:var(--text)}
body.dropping::after{content:"Drop files to attach";position:fixed;inset:12px;border:2px dashed var(--accent);border-radius:20px;background:color-mix(in srgb,var(--bg) 85%,transparent);display:flex;align-items:center;justify-content:center;font-size:20px;z-index:200;pointer-events:none}

/* ---------- popover + toast ---------- */
.pop{position:fixed;z-index:300;background:var(--elev);border:1px solid var(--border);border-radius:14px;box-shadow:var(--shadow);padding:6px;max-height:min(60vh,420px);overflow-y:auto}
.pop-item{display:flex;align-items:center;gap:10px;width:100%;text-align:left;padding:9px 11px;border-radius:9px}
.pop-item:hover{background:var(--hover)}
.pop-item .pi-main{flex:1;min-width:0}
.pop-item b{display:block;font-size:14.5px;font-weight:500;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.pop-item small{display:block;color:var(--text3);font-size:12.5px;margin-top:1px}
.pop-item.on svg{color:var(--accent)}
.toast{position:fixed;left:50%;bottom:calc(90px + env(safe-area-inset-bottom));transform:translate(-50%,20px);background:var(--text);color:var(--bg);padding:8px 16px;border-radius:20px;font-size:13.5px;opacity:0;pointer-events:none;transition:.2s;z-index:400}
.toast.on{opacity:1;transform:translate(-50%,0)}

/* ---------- artifact / preview panel ---------- */
.pv{display:flex;flex-direction:column;background:var(--bg);border-left:1px solid var(--border);width:min(52vw,820px);flex:none;z-index:60}
.pv-top{display:flex;align-items:center;gap:4px;padding:8px 8px 8px 14px;border-bottom:1px solid var(--border);height:54px;flex:none}
.pv-title{display:flex;align-items:center;gap:9px;flex:1;min-width:0;color:var(--text2)}
.pv-title b{font-weight:500;color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;font-size:14.5px}
.seg{display:flex;background:var(--side);border:1px solid var(--border);border-radius:10px;padding:2px;margin-right:4px}
.seg button{height:28px;padding:0 12px;border-radius:8px;font-size:13px;color:var(--text3)}
.seg button.on{background:var(--elev);color:var(--text);box-shadow:0 1px 3px rgba(0,0,0,.2)}
.pv-body{flex:1;min-height:0;position:relative;background:var(--bg)}
.pv-body iframe{position:absolute;inset:0;width:100%;height:100%;border:0;background:#fff}
.pv-body pre{position:absolute;inset:0;margin:0;padding:14px;overflow:auto;font-size:12.5px;line-height:1.55;white-space:pre;color:var(--text)}
.pv-media{position:absolute;inset:0;overflow:auto;display:flex;align-items:center;justify-content:center;padding:14px}
.pv-media img,.pv-media video{max-width:100%;max-height:100%;border-radius:10px}
.pv-media .bot{align-self:flex-start;width:100%;margin:0}
.pv-note{position:absolute;left:12px;right:12px;bottom:12px;padding:8px 12px;border-radius:10px;background:var(--elev);border:1px solid var(--border);font-size:13px;color:var(--text2)}

/* ---------- file manager ---------- */
.fm{position:fixed;top:0;right:0;bottom:0;width:min(440px,100vw);background:var(--bg);border-left:1px solid var(--border);display:flex;flex-direction:column;z-index:120;box-shadow:var(--shadow)}
.fm-top{display:flex;align-items:center;gap:10px;padding:10px 10px 10px 16px;border-bottom:1px solid var(--border);height:54px}
.fm-crumb{flex:1;color:var(--text3);font-family:var(--mono);font-size:12.5px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fm-bar{display:flex;gap:6px;padding:10px 12px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.fm-list{flex:1;overflow:auto;padding:6px}
.fm-row{display:flex;align-items:center;gap:11px;padding:9px 10px;border-radius:9px;cursor:pointer;font-size:14.5px}
.fm-row:hover{background:var(--hover)}
.fm-row .ic{color:var(--text3);display:flex}
.fm-row .ic svg{width:17px;height:17px}
.fm-row .nm{flex:1;min-width:0;white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fm-row .sz{color:var(--text3);font-size:12px}
.fm-row .ops{display:flex;gap:2px}
.fm-row .ops button{width:30px;height:30px;border-radius:7px;color:var(--text3);display:flex;align-items:center;justify-content:center}
.fm-row .ops button:hover{background:var(--bg);color:var(--text)}
.fm-row .ops svg{width:15px;height:15px;pointer-events:none}
.fm-empty{padding:26px;text-align:center;color:var(--text3)}
.fm-editor{flex:1;display:flex;flex-direction:column;min-height:0}
.fm-editor-top{display:flex;align-items:center;gap:6px;padding:8px 10px;border-bottom:1px solid var(--border);flex-wrap:wrap}
.fm-editor-top span{flex:1;min-width:120px;font-family:var(--mono);font-size:12.5px;color:var(--text2);white-space:nowrap;overflow:hidden;text-overflow:ellipsis}
.fm-editor textarea{flex:1;resize:none;border:0;outline:0;padding:14px;background:var(--bg);color:var(--text);font:13px/1.55 var(--mono)}
.fm-preview{flex:1;overflow:auto;padding:14px;text-align:center}
.fm-preview img{max-width:100%;border-radius:10px}

/* ---------- dialogs ---------- */
dialog{background:var(--elev);color:var(--text);border:1px solid var(--border);border-radius:20px;padding:22px;width:min(520px,calc(100vw - 24px));max-height:88dvh;box-shadow:var(--shadow)}
dialog::backdrop{background:rgba(0,0,0,.55)}
dialog h3{margin:0 0 4px;font:400 24px var(--serif)}
dialog label{display:block;font-size:13px;color:var(--text3);margin-top:12px}
dialog label.chk{display:flex;align-items:center;gap:9px;color:var(--text2);font-size:14.5px}
dialog input:not([type=checkbox]),dialog textarea{display:block;width:100%;margin-top:5px;padding:9px 12px;border-radius:10px;border:1px solid var(--border);background:var(--bg);color:var(--text);font:15px var(--sans);outline:none}
dialog input:focus,dialog textarea:focus{border-color:var(--accent)}
dialog .row{display:flex;gap:10px}
dialog .row>*{flex:1;min-width:0}
dialog .acts{display:flex;gap:8px;justify-content:flex-end;margin-top:18px}
.mems{list-style:none;padding:0;margin:12px 0 0;max-height:46vh;overflow:auto}
.mems li{display:flex;gap:8px;align-items:flex-start;padding:9px 0;border-bottom:1px solid var(--border);font-size:14.5px}
.mems li span{flex:1}
.mems li button{color:var(--text3);width:26px;height:26px;border-radius:6px}
.mems li button:hover{color:var(--danger)}

/* ---------- responsive ---------- */
@media (max-width:860px){
  .only-mobile{display:inline-flex}
  .side{position:fixed;inset:0 auto 0 0;width:min(320px,86vw);transform:translateX(-102%);transition:transform .25s ease;box-shadow:var(--shadow)}
  .side.open{transform:none}
  .scrim{display:block;position:fixed;inset:0;background:rgba(0,0,0,.5);opacity:0;pointer-events:none;transition:opacity .25s;z-index:45}
  .app.menu-open .scrim{opacity:1;pointer-events:auto}
  .conv button{opacity:1}
}
@media (max-width:1000px){
  .pv{position:fixed;inset:0;width:100%;border:0;z-index:130}
}
@media (max-width:520px){
  #pvDl{display:none}
  .seg button{padding:0 9px}
  .msgs{padding:4px 14px 24px}
  .bot{font-size:16.5px}
  .chip-btn{padding:0 9px}
}
`;

const BODY = `
<section id="login" class="login hidden"><form id="loginForm">
  <div class="spark-lg" data-ic="spark"></div>
  <h1>Power Agent</h1>
  <p class="muted">Enter the password from your launcher console<br>or <code>.power-agent/.env</code></p>
  <input id="pw" type="password" placeholder="Password" autocomplete="current-password" autofocus>
  <button class="btn accent wide">Continue</button><p id="loginErr" class="err"></p>
</form></section>

<div id="app" class="app hidden">
  <div id="scrim" class="scrim"></div>
  <aside id="side" class="side">
    <div class="side-top">
      <div class="brand"><span class="mark" data-ic="spark"></span><span class="wordmark">Power Agent</span></div>
      <button id="sideClose" class="icon-btn only-mobile" data-ic="x" aria-label="Close menu"></button>
    </div>
    <button id="newChat" class="newchat"><span class="plus" data-ic="plus"></span>New chat</button>
    <nav class="nav">
      <button id="openFiles" class="nav-item"><span data-ic="folder"></span>Files</button>
      <button id="openMem" class="nav-item"><span data-ic="bookmark"></span>Memory</button>
    </nav>
    <div class="side-label">Recents</div>
    <nav id="convs" class="convs"></nav>
    <div class="side-foot">
      <div class="who"><span class="avatar" data-ic="user"></span><div><b>You</b><small id="whoSub">Local agent</small></div></div>
      <button id="themeBtn" class="icon-btn" title="Toggle theme" aria-label="Toggle theme"></button>
      <button id="openSet" class="icon-btn" data-ic="gear" title="Settings" aria-label="Settings"></button>
      <button id="logout" class="icon-btn" data-ic="logout" title="Log out" aria-label="Log out"></button>
    </div>
  </aside>

  <main id="main" class="main">
    <header class="top">
      <button id="menu" class="icon-btn only-mobile" data-ic="menu" aria-label="Open menu"></button>
      <div id="title" class="title">New chat</div>
      <button id="filesBtn" class="icon-btn" data-ic="folder" title="Files" aria-label="Files"></button>
      <button id="ctxBtn" class="ring-btn" title="Context usage"><svg viewBox="0 0 36 36"><circle class="ring-bg" cx="18" cy="18" r="15.9155"/><circle id="ringBar" class="ring-fg" cx="18" cy="18" r="15.9155" stroke-dasharray="0 100"/></svg></button>
    </header>
    <div id="banner" class="banner hidden"><span id="bnFree"><b>Setup:</b> open the <a id="dash" target="_blank" rel="noopener">FreeLLMAPI dashboard</a>, add provider keys, copy the <b>unified key</b>, paste it in <button class="link" id="bannerSet">Settings</button>.</span><span id="bnExt" class="hidden"><b>Setup:</b> open <button class="link" id="bannerSet2">Settings</button>, paste your API key and API base URL (OpenRouter, Groq, OpenAI...), then pick a model.</span></div>
    <div id="msgs" class="msgs"></div>
    <button id="toBottom" class="to-bottom hidden" data-ic="arrowDown" aria-label="Scroll to bottom"></button>
    <div class="dock">
      <div id="todoWrap" class="hidden"></div>
      <div id="cmds" class="cmds hidden"></div>
      <form id="composer" class="composer">
        <div id="attach" class="attach hidden"></div>
        <textarea id="input" rows="1" placeholder="Reply to Power Agent..."></textarea>
        <div class="crow">
          <button id="clip" type="button" class="circle-btn" data-ic="plus" title="Attach files" aria-label="Attach files"></button>
          <button id="modeChip" type="button" class="chip-btn" data-mode="yolo"><span data-ic="shield"></span><span id="modeLbl">Auto-allow</span></button>
          <span class="grow"></span>
          <button id="modelChip" type="button" class="chip-btn plain"><span id="modelLbl">auto</span><span data-ic="chevD"></span></button>
          <button id="send" class="send" data-ic="arrowUp" aria-label="Send"></button>
          <button id="stop" type="button" class="send stop hidden" data-ic="stop" aria-label="Stop"></button>
        </div>
      </form>
      <input id="filePick" type="file" multiple hidden>
      <p class="fine">Power Agent works directly on your machine. Check important results.</p>
    </div>
  </main>

  <aside id="pv" class="pv hidden">
    <header class="pv-top">
      <div class="pv-title"><span data-ic="file"></span><b id="pvName"></b></div>
      <div class="seg"><button id="pvTabPrev" class="on">Preview</button><button id="pvTabCode">Code</button></div>
      <button id="pvReload" class="icon-btn" data-ic="refresh" title="Reload" aria-label="Reload"></button>
      <a id="pvOpen" class="icon-btn" data-ic="ext" target="_blank" rel="noopener" title="Open in new tab"></a>
      <a id="pvDl" class="icon-btn" data-ic="download" title="Download"></a>
      <button id="pvClose" class="icon-btn" data-ic="x" title="Close" aria-label="Close"></button>
    </header>
    <div class="pv-body">
      <iframe id="pvFrame" class="hidden" sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-modals allow-pointer-lock allow-downloads" allow="autoplay; fullscreen; clipboard-write; gamepad; accelerometer; gyroscope" allowfullscreen></iframe>
      <div id="pvMedia" class="pv-media hidden"></div>
      <pre id="pvCode" class="hidden"></pre>
      <div id="pvNote" class="pv-note hidden"></div>
    </div>
  </aside>
</div>

<aside id="fm" class="fm hidden">
  <header class="fm-top">
    <b>Files</b><span id="fmCrumb" class="fm-crumb"></span>
    <button id="fmClose" class="icon-btn" data-ic="x" aria-label="Close"></button>
  </header>
  <div class="fm-bar">
    <button id="fmNewFile" class="btn sm">New file</button>
    <button id="fmNewDir" class="btn sm">New folder</button>
    <button id="fmUpload" class="btn sm">Upload</button>
    <input id="fmFile" type="file" multiple hidden>
  </div>
  <div id="fmList" class="fm-list"></div>
  <div id="fmEditor" class="fm-editor hidden">
    <div class="fm-editor-top">
      <span id="fmName"></span>
      <button id="fmSave" class="btn primary sm">Save</button>
      <button id="fmPrev" class="btn sm hidden">Preview</button>
      <button id="fmAsk" class="btn sm">Ask</button>
      <button id="fmDl" class="btn sm">Download</button>
      <button id="fmBack" class="btn ghost sm">Back</button>
    </div>
    <textarea id="fmText" spellcheck="false"></textarea>
    <div id="fmPreview" class="fm-preview hidden"></div>
  </div>
</aside>

<div id="pop" class="pop hidden"></div>
<div id="toast" class="toast"></div>

<dialog id="setDlg"><form id="setForm" method="dialog">
  <h3>Settings</h3>
  <label>API key (FreeLLMAPI unified key or provider key)<input name="apiKey" placeholder="freellmapi-... / sk-..." autocomplete="off"></label>
  <label>API base URL (optional, any OpenAI-compatible API)<input name="baseUrl" placeholder="empty = FreeLLMAPI. e.g. https://openrouter.ai/api/v1" autocomplete="off" inputmode="url"></label>
  <label>Image model<input name="imageModel" placeholder="empty = auto"></label>
  <div class="row">
    <label>Max steps (0 = unlimited)<input name="maxSteps" type="number" min="0" step="1"></label>
    <label>Temperature<input name="temperature" type="number" step="0.1" min="0" max="2"></label>
  </div>
  <div class="row">
    <label>Context budget (0 = auto)<input name="tokenBudget" type="number" min="0" step="1000"></label>
    <label>Parallel tools<input name="maxParallel" type="number" min="1" max="32"></label>
  </div>
  <label class="chk"><input name="allowShell" type="checkbox"> Allow shell commands</label>
  <label>Fallback models (comma separated)<input name="fallbackModels" placeholder="auto:fast"></label>
  <label>Custom instructions<textarea name="persona" rows="4" placeholder="e.g. Always answer in Taglish. Node + Postgres."></textarea></label>
  <p id="setMsg" class="muted"></p>
  <div class="acts"><button value="cancel" class="btn ghost">Close</button><button id="saveSet" type="button" class="btn primary">Save</button></div>
</form></dialog>

<dialog id="memDlg"><form method="dialog">
  <h3>Memory</h3><p class="muted" style="margin:0">Saved facts are recalled in every chat.</p>
  <div class="row" style="margin-top:12px"><input id="memIn" placeholder="Add a memory..."><button id="memAdd" type="button" class="btn primary" style="flex:none">Add</button></div>
  <ul id="mems" class="mems"></ul>
  <div class="acts"><button value="cancel" class="btn ghost">Close</button></div>
</form></dialog>
`;

function clientApp() {
  const $ = (id) => document.getElementById(id);
  const api = async (url, opts = {}) => {
    const r = await fetch(url, { ...opts, headers: { 'content-type': 'application/json', ...(opts.headers || {}) } });
    if (r.status === 401 && !url.includes('login')) { showLogin(); throw new Error('Login required'); }
    const j = await r.json().catch(() => ({}));
    if (!r.ok) throw new Error(j.error || 'HTTP ' + r.status);
    return j;
  };

  // ---------- icons (inline SVG, no emoji, no font glyph dependencies) ----------
  const spark = (() => {
    let d = '';
    for (let i = 0; i < 6; i++) { const a = (i * Math.PI) / 6, x = Math.cos(a) * 9.6, y = Math.sin(a) * 9.6; d += 'M' + (12 - x).toFixed(2) + ' ' + (12 - y).toFixed(2) + 'L' + (12 + x).toFixed(2) + ' ' + (12 + y).toFixed(2); }
    return '<path d="' + d + '" stroke-width="2.7"/>';
  })();
  const IC = {
    spark, menu: '<path d="M4 6.5h16M4 12h16M4 17.5h16"/>', plus: '<path d="M12 5v14M5 12h14"/>', x: '<path d="M6 6l12 12M18 6L6 18"/>',
    arrowUp: '<path d="M12 19V5M5.5 11.5L12 5l6.5 6.5"/>', arrowDown: '<path d="M12 5v14M5.5 12.5L12 19l6.5-6.5"/>', stop: '<rect x="6.5" y="6.5" width="11" height="11" rx="2" fill="currentColor"/>',
    chevD: '<path d="M6 9.5l6 6 6-6"/>', chevR: '<path d="M9.5 6l6 6-6 6"/>', check: '<path d="M5 12.5l4.5 4.5L19 7.5"/>',
    copy: '<rect x="9" y="9" width="11" height="11" rx="2.2"/><path d="M5 15V6.2A2.2 2.2 0 017.2 4H16"/>', refresh: '<path d="M20 11a8 8 0 10-2.4 5.7M20 4v7h-7"/>',
    file: '<path d="M14 3H7.5A1.5 1.5 0 006 4.5v15A1.5 1.5 0 007.5 21h9a1.5 1.5 0 001.5-1.5V7z"/><path d="M14 3v4h4"/>',
    folder: '<path d="M3 7.5A1.5 1.5 0 014.5 6H9l2 2.2h8.5A1.5 1.5 0 0121 9.7v8.8a1.5 1.5 0 01-1.5 1.5h-15A1.5 1.5 0 013 18.5z"/>',
    gear: '<circle cx="12" cy="12" r="3"/><path d="M12 2.8v2.4M12 18.8v2.4M4.5 7.4l2 1.2M17.5 15.4l2 1.2M4.5 16.6l2-1.2M17.5 8.6l2-1.2M2.8 12h2.4M18.8 12h2.4" /><circle cx="12" cy="12" r="7.3"/>',
    bookmark: '<path d="M6.5 3.5h11v17L12 16.5l-5.5 4z"/>', trash: '<path d="M4.5 7h15M10 11v6M14 11v6M6.5 7l.9 12.5h9.2L17.5 7M9 7V4.5h6V7"/>',
    terminal: '<path d="M5 7.5l4.5 4.5L5 16.5M12 17h7"/>', pencil: '<path d="M4 20h4L19 9l-4-4L4 16z"/>', search: '<circle cx="11" cy="11" r="6.5"/><path d="M20 20l-4.3-4.3"/>',
    globe: '<circle cx="12" cy="12" r="9"/><path d="M3 12h18M12 3c3 3.2 3 14.8 0 18M12 3c-3 3.2-3 14.8 0 18"/>', download: '<path d="M12 4v11M7 11l5 5 5-5M5 20h14"/>',
    ext: '<path d="M14 4h6v6M20 4l-9 9M18 14v4.5a1.5 1.5 0 01-1.5 1.5h-11A1.5 1.5 0 014 18.5v-11A1.5 1.5 0 015.5 6H10"/>', code: '<path d="M8 7l-5 5 5 5M16 7l5 5-5 5M14 4.5l-4 15"/>',
    logout: '<path d="M9 4H5.5A1.5 1.5 0 004 5.5v13A1.5 1.5 0 005.5 20H9M16 8l4 4-4 4M20 12H9"/>', shield: '<path d="M12 3l7 3v5.5c0 4.5-3 7.8-7 9.5-4-1.7-7-5-7-9.5V6z"/>',
    list: '<path d="M9 6.5h11M9 12h11M9 17.5h11M4 6.5l1 1 2-2M4 12l1 1 2-2M4 17.5l1 1 2-2"/>', user: '<circle cx="12" cy="8.5" r="3.8"/><path d="M4.5 20.5a7.5 7.5 0 0115 0"/>',
    sun: '<circle cx="12" cy="12" r="4"/><path d="M12 2.5v2.2M12 19.3v2.2M2.5 12h2.2M19.3 12h2.2M5.3 5.3l1.6 1.6M17.1 17.1l1.6 1.6M18.7 5.3l-1.6 1.6M6.9 17.1l-1.6 1.6"/>', moon: '<path d="M20 14.5A8 8 0 019.5 4a8 8 0 1010.5 10.5z"/>',
    eye: '<path d="M2.5 12S6 5.5 12 5.5 21.5 12 21.5 12 18 18.5 12 18.5 2.5 12 2.5 12z"/><circle cx="12" cy="12" r="2.8"/>', brain: '<path d="M12 5a3 3 0 00-5.6 1.4A3.4 3.4 0 004 9.6a3.3 3.3 0 001 2.4A3.6 3.6 0 007 18a3 3 0 005 1.6V5zM12 5a3 3 0 015.6 1.4A3.4 3.4 0 0120 9.6a3.3 3.3 0 01-1 2.4 3.6 3.6 0 01-2 6 3 3 0 01-5 1.6"/>',
    play: '<path d="M8 5.5v13l11-6.5z" fill="currentColor"/>', image: '<rect x="3.5" y="4.5" width="17" height="15" rx="2"/><circle cx="9" cy="10" r="1.6"/><path d="M4 17l5-4.5 4 3.5 3-2.5 4 3.5"/>',
    wand: '<path d="M5 19L16 8M14 4v3M19 9h-3M17.5 5.5l-1.5 1.5M8 5v2M7 6H5"/>',
  };
  const ic = (n, cls) => '<svg class="ic' + (cls ? ' ' + cls : '') + '" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' + (IC[n] || '') + '</svg>';
  document.querySelectorAll('[data-ic]').forEach((e) => { e.innerHTML = ic(e.dataset.ic); });

  // ---------- state ----------
  let convId = localStorage.getItem('pa_conv') || null, busy = false, ctxChars = 0, ctxBudget = 200000, attach = [];
  let curMode = 'yolo', curModel = 'auto', models = [], stick = true, lastUser = '', todoOpen = true, artifacts = new Map();
  const COARSE = matchMedia('(pointer: coarse)').matches;
  const SUGGEST = [
    ['code', 'Build an app', 'Build a complete web app: '], ['wand', 'Make a video', 'Make a real MP4 video (install whatever tools you need): '],
    ['search', 'Research', 'Research this on the web and give me a sourced summary: '], ['terminal', 'Automate', 'Write and run a script that automates: '],
    ['eye', 'Fix a bug', 'Find and fix the bug in my project: '], ['folder', 'Explore code', '/init document this codebase for future sessions'],
  ];
  const COMMANDS = [['/help', 'Show all commands'], ['/plan', 'Read-only planning mode'], ['/act', 'Back to auto-allow'], ['/init', 'Document this codebase'], ['/compact', 'Summarize old context'], ['/undo', 'Undo the last file changes'], ['/review', 'Review recent changes'], ['/test', 'Run the tests'], ['/todos', 'Show the task list'], ['/memory', 'Show saved memory'], ['/model', 'Show or set the model'], ['/clear', 'Clear this chat']];
  const MODES = [
    { value: 'yolo', label: 'Auto-allow', desc: 'The agent decides and acts. No prompts.' },
    { value: 'acceptEdits', label: 'Accept edits', desc: 'Edits are automatic, commands ask first.' },
    { value: 'ask', label: 'Ask first', desc: 'Asks before edits and commands.' },
    { value: 'plan', label: 'Plan', desc: 'Read-only. Investigates, then proposes a plan.' },
  ];

  // ---------- markdown ----------
  const esc = (s) => String(s).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  const safeUrl = (u) => (/^(https?:\/\/|\/files\/)/i.test(u) ? u : '#');
  function inline(s) {
    const codes = [];
    s = s.replace(/`([^`]+)`/g, (_, c) => { codes.push(c); return '\u0000' + (codes.length - 1) + '\u0000'; });
    s = esc(s)
      .replace(/!\[([^\]]*)\]\(([^)\s]+)\)/g, (_, a, u) => '<img alt="' + a + '" src="' + safeUrl(u) + '">')
      .replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, (_, t, u) => '<a href="' + safeUrl(u) + '" target="_blank" rel="noopener">' + t + '</a>')
      .replace(/(^|[\s(])(https?:\/\/[^\s<)]+)/g, '$1<a href="$2" target="_blank" rel="noopener">$2</a>')
      .replace(/\*\*([^*]+)\*\*/g, '<b>$1</b>').replace(/(^|[^*])\*([^*\s][^*]*)\*/g, '$1<i>$2</i>').replace(/~~([^~]+)~~/g, '<s>$1</s>');
    return s.replace(/\u0000(\d+)\u0000/g, (_, i) => '<code>' + esc(codes[i]) + '</code>');
  }
  function md(src) {
    const out = [], lines = String(src).replace(/\r/g, '').split('\n');
    let i = 0;
    while (i < lines.length) {
      const l = lines[i], fence = l.match(/^\s*```(\S*)/);
      if (fence) {
        const buf = []; i++;
        while (i < lines.length && !/^\s*```/.test(lines[i])) buf.push(lines[i++]);
        i++;
        out.push('<pre><div class="ch"><span>' + esc(fence[1] || 'code') + '</span><button class="copy" type="button">Copy</button></div><code>' + esc(buf.join('\n')) + '</code></pre>');
        continue;
      }
      const h = l.match(/^(#{1,4})\s+(.*)/);
      if (h) { out.push('<h' + (h[1].length + 1) + '>' + inline(h[2]) + '</h' + (h[1].length + 1) + '>'); i++; continue; }
      if (/^\s*\|.*\|\s*$/.test(l) && /^\s*\|?\s*:?-{2,}/.test(lines[i + 1] || '')) {
        const row = (r) => r.trim().replace(/^\||\|$/g, '').split('|').map((c) => c.trim());
        let t = '<table><tr>' + row(l).map((c) => '<th>' + inline(c) + '</th>').join('') + '</tr>';
        i += 2;
        while (i < lines.length && /^\s*\|/.test(lines[i])) t += '<tr>' + row(lines[i++]).map((c) => '<td>' + inline(c) + '</td>').join('') + '</tr>';
        out.push(t + '</table>');
        continue;
      }
      if (/^\s*>\s?/.test(l)) {
        const q = [];
        while (i < lines.length && /^\s*>\s?/.test(lines[i])) q.push(lines[i++].replace(/^\s*>\s?/, ''));
        out.push('<blockquote>' + q.map(inline).join('<br>') + '</blockquote>');
        continue;
      }
      if (/^\s*([-*+]|\d+[.)])\s+/.test(l)) {
        const ordered = /^\s*\d/.test(l), items = [];
        while (i < lines.length && /^\s*([-*+]|\d+[.)])\s+/.test(lines[i])) items.push(lines[i++].replace(/^\s*([-*+]|\d+[.)])\s+/, ''));
        out.push((ordered ? '<ol>' : '<ul>') + items.map((x) => '<li>' + inline(x) + '</li>').join('') + (ordered ? '</ol>' : '</ul>'));
        continue;
      }
      if (!l.trim()) { i++; continue; }
      const para = [];
      while (i < lines.length && lines[i].trim() && !/^\s*(```|#{1,4}\s|[-*+]\s|\d+[.)]\s|\||>)/.test(lines[i])) para.push(lines[i++]);
      if (!para.length) para.push(lines[i++]);
      out.push('<p>' + para.map(inline).join('<br>') + '</p>');
    }
    return out.join('');
  }

  // ---------- helpers ----------
  const msgs = $('msgs');
  const el = (tag, cls, html) => { const e = document.createElement(tag); if (cls) e.className = cls; if (html != null) e.innerHTML = html; return e; };
  const scroll = (force) => { if (force || stick) msgs.scrollTop = msgs.scrollHeight; };
  msgs.addEventListener('scroll', () => { stick = msgs.scrollHeight - msgs.scrollTop - msgs.clientHeight < 140; $('toBottom').classList.toggle('hidden', stick); });
  $('toBottom').onclick = () => { stick = true; scroll(true); };
  let toastT;
  const toast = (t) => { const e = $('toast'); e.textContent = t; e.classList.add('on'); clearTimeout(toastT); toastT = setTimeout(() => e.classList.remove('on'), 1600); };
  const meter = () => {
    const pct = Math.min(100, ((ctxChars / 3.6) / Math.max(1000, ctxBudget)) * 100);
    const bar = $('ringBar'); bar.setAttribute('stroke-dasharray', pct.toFixed(1) + ' 100'); bar.classList.toggle('warn', pct > 75);
    $('ctxBtn').title = 'Context about ' + Math.round(pct) + '% of ' + Math.round(ctxBudget / 1000) + 'k tokens (estimate)';
  };
  $('ctxBtn').onclick = () => toast($('ctxBtn').title);
  const encPath = (s) => String(s).split('/').map(encodeURIComponent).join('/');
  const extOf = (p) => (String(p).split('?')[0].split('.').pop() || '').toLowerCase();
  const PREVIEWABLE = /^(html?|svg|png|jpe?g|gif|webp|avif|mp4|m4v|webm|mov|mp3|wav|ogg|m4a|pdf|md)$/;
  const kindOf = (p) => { const e = extOf(p); return /^html?$/.test(e) ? 'Web page' : /^(svg|png|jpe?g|gif|webp|avif)$/.test(e) ? 'Image' : /^(mp4|m4v|webm|mov)$/.test(e) ? 'Video' : /^(mp3|wav|ogg|m4a)$/.test(e) ? 'Audio' : e === 'pdf' ? 'PDF' : e === 'md' ? 'Document' : 'File'; };

  // ---------- popover menus ----------
  const outside = (e) => { if (!$('pop').contains(e.target)) closeMenu(); };
  function closeMenu() { $('pop').classList.add('hidden'); document.removeEventListener('pointerdown', outside); }
  function openMenu(anchor, items, current, onPick) {
    const pop = $('pop'); pop.innerHTML = '';
    items.forEach((it) => {
      const b = el('button', 'pop-item' + (it.value === current ? ' on' : ''));
      b.type = 'button';
      b.innerHTML = '<span class="pi-main"><b></b><small></small></span>' + (it.value === current ? ic('check') : '');
      b.querySelector('b').textContent = it.label;
      const sm = b.querySelector('small'); if (it.desc) sm.textContent = it.desc; else sm.remove();
      b.onclick = () => { closeMenu(); onPick(it.value); };
      pop.append(b);
    });
    pop.classList.remove('hidden');
    const r = anchor.getBoundingClientRect(), w = Math.min(330, innerWidth - 24);
    pop.style.width = w + 'px';
    pop.style.left = Math.max(12, Math.min(innerWidth - w - 12, r.left)) + 'px';
    const h = pop.offsetHeight, above = r.top > h + 24;
    pop.style.top = (above ? Math.max(8, r.top - h - 8) : Math.min(innerHeight - h - 8, r.bottom + 8)) + 'px';
    setTimeout(() => document.addEventListener('pointerdown', outside), 0);
  }

  // ---------- empty state / messages ----------
  const greet = () => { const h = new Date().getHours(); return h < 5 ? 'Burning the midnight oil' : h < 12 ? 'Good morning' : h < 18 ? 'Good afternoon' : 'Good evening'; };
  function emptyState() {
    msgs.innerHTML = ''; artifacts = new Map();
    const e = el('div', 'empty', '<div class="hello">' + ic('spark') + '<h2>' + greet() + '</h2></div><p>What should we build?</p>');
    const chips = el('div', 'chips');
    SUGGEST.forEach(([icon, label, prompt]) => {
      const c = el('button', 'chip', ic(icon) + '<span></span>'); c.type = 'button'; c.querySelector('span').textContent = label;
      c.onclick = () => { $('input').value = prompt; autosize(); $('input').focus(); };
      chips.append(c);
    });
    e.append(chips); msgs.append(e);
  }
  const addUser = (t) => {
    if (/^\[auto\]/.test(t)) { const n = el('div', 'auto-note', '<span>Auto-continue</span>'); msgs.append(n); return; }
    const d = el('div', 'user', '<span class="avatar">' + ic('user') + '</span><div class="ub"></div>');
    d.querySelector('.ub').textContent = t; msgs.append(d); ctxChars += t.length; meter();
  };
  function enhance(root) {
    root.querySelectorAll('a[href^="/files/"]').forEach((a) => {
      const p = decodeURIComponent(a.getAttribute('href').slice(7).split('?')[0]);
      if (PREVIEWABLE.test(extOf(p))) { a.dataset.path = p; a.classList.add('artlink'); a.removeAttribute('target'); }
    });
    root.querySelectorAll('code').forEach((c) => {
      if (c.closest('pre') || c.closest('a')) return;
      const m = c.textContent.trim().match(/^\/files\/(.+\.(?:html?|mp4|webm|mov|png|jpe?g|gif|webp|svg|pdf|md))$/i);
      if (!m) return;
      const a = el('a', 'artlink'); a.href = c.textContent.trim(); a.dataset.path = decodeURIComponent(m[1]);
      c.replaceWith(a); a.append(c);
    });
    const seen = new Set(root.querySelectorAll('video').length ? ['x'] : []);
    root.querySelectorAll('a[data-path]').forEach((a) => {
      const p = a.dataset.path;
      if (/^(mp4|m4v|webm|mov)$/.test(extOf(p)) && !seen.has(p)) {
        seen.add(p);
        const v = el('video'); v.controls = true; v.playsInline = true; v.preload = 'metadata'; v.src = '/files/' + encPath(p);
        (a.closest('p,li') || a).after(v);
      }
    });
  }
  function botActions(botEl, text) {
    const a = el('div', 'macts', '<button type="button" class="a-copy" title="Copy">' + ic('copy') + '</button><button type="button" class="a-retry" title="Retry">' + ic('refresh') + '</button><span class="sp">' + ic('spark') + '</span>');
    a.querySelector('.a-copy').onclick = () => { navigator.clipboard?.writeText(text); toast('Copied'); };
    a.querySelector('.a-retry').onclick = () => { if (!busy && lastUser) sendMsg(lastUser); };
    botEl.after(a);
  }
  function addBot(t, actions) { const b = el('div', 'bot', md(t)); msgs.append(b); enhance(b); if (actions) botActions(b, t); ctxChars += t.length; meter(); return b; }

  // ---------- steps / tool rows ----------
  const TOOL_LABEL = {
    run_command: ['Ran', 'terminal'], run_python: ['Ran Python', 'terminal'], read_file: ['Read', 'file'], create_file: ['Created', 'file'], write_file: ['Wrote', 'file'],
    apply_patch: ['Edited', 'pencil'], multi_edit: ['Edited', 'pencil'], list_files: ['Listed', 'folder'], search_code: ['Searched code', 'search'], web_search: ['Searched the web', 'globe'],
    fetch_url: ['Fetched', 'globe'], http_request: ['HTTP request', 'globe'], todo_write: ['Updated tasks', 'list'], spawn_agent: ['Subagent', 'spark'], screenshot: ['Screenshot', 'eye'],
    think: ['Thinking', 'brain'], install_package: ['Installing', 'download'], env_info: ['Checked the machine', 'terminal'], define_tool: ['Created a tool', 'code'], mcp_add: ['Connected MCP server', 'code'], git: ['Git', 'code'],
  };
  function toolInfo(name, args) {
    let o = {}; try { o = JSON.parse(args || '{}'); } catch { /* partial */ }
    const main = o.command || o.path || o.file_path || o.url || o.query || (Array.isArray(o.names) ? o.names.join(' ') : '') || o.task || o.name || o.thought || o.pattern || Object.values(o).find((v) => typeof v === 'string') || '';
    const t = TOOL_LABEL[name] || [name.replace(/^mcp_/, '').replace(/_/g, ' '), 'code'];
    return { label: t[0], icon: t[1], main: String(main).replace(/\s+/g, ' ').slice(0, 160), path: o.path || o.file_path || '' };
  }
  function newGroup() {
    const d = el('details', 'steps', '<summary><span class="st"></span><span class="lbl">Working</span>' + ic('chevR', 'chev') + '</summary><div class="steps-body"></div>');
    d.dataset.n = '0'; d.dataset.run = '0'; return d;
  }
  function refreshGroup(g) {
    const n = +g.dataset.n, run = +g.dataset.run, last = g._last || { label: 'Working', main: '' };
    g.classList.toggle('running', run > 0);
    g.querySelector('.st').innerHTML = run ? '<span class="spin"></span>' : ic('check');
    g.querySelector('.lbl').textContent = run ? last.label + (last.main ? ': ' + last.main : '') : n === 1 ? last.label + (last.main ? ': ' + last.main : '') : 'Used ' + n + ' tools';
  }
  const toolState = (d, out) => {
    const bad = out != null && /^(Error|Blocked|exit: (?!0\b))/i.test(String(out).trim());
    d.classList.toggle('ok', out != null && !bad); d.classList.toggle('bad', bad);
    d.querySelector('.ti').innerHTML = out == null ? '<span class="spin"></span>' : bad ? ic('x') : ic(d._icon);
  };
  function paintDiff(pre) {
    pre.innerHTML = pre.textContent.split('\n').map((l) => /^\+/.test(l) && !/^\+\+\+/.test(l) ? '<span class="add">' + esc(l) + '</span>' : /^-/.test(l) && !/^---/.test(l) ? '<span class="del">' + esc(l) + '</span>' : esc(l)).join('\n');
  }
  function toolRow(g, id, name, args, output) {
    const t = toolInfo(name, args);
    const d = el('details', 'tool'); d.dataset.id = id; d.dataset.name = name; d._icon = t.icon;
    d.innerHTML = '<summary><span class="ti"></span><span class="tl"></span><span class="ta"></span>' + ic('chevR', 'chev') + '</summary>';
    d.querySelector('.tl').textContent = t.label; d.querySelector('.ta').textContent = t.main;
    const pre = el('pre'); pre.textContent = 'args: ' + (args || '{}') + (output == null ? '' : '\n\n' + output);
    d.append(pre);
    if (output != null && /^(\+|-)/m.test(output)) paintDiff(pre);
    g.querySelector('.steps-body').append(d);
    g.dataset.n = +g.dataset.n + 1; if (output == null) g.dataset.run = +g.dataset.run + 1;
    g._last = t; toolState(d, output); refreshGroup(g);
    if (output != null) artifactFor(g, name, args, output);
    return d;
  }
  function finishTool(id, output, name, args) {
    const d = msgs.querySelector('details.tool[data-id="' + CSS.escape(id) + '"]'); if (!d) return;
    const g = d.closest('.steps'); const pre = d.querySelector('pre'); pre.textContent += '\n\n' + output;
    if (/^(\+|-)/m.test(output)) paintDiff(pre);
    toolState(d, output); g.dataset.run = Math.max(0, +g.dataset.run - 1); refreshGroup(g);
    artifactFor(g, name || d.dataset.name, args, output);
  }
  function artifactFor(g, name, args, output) {
    const paths = [];
    const t = toolInfo(name, args);
    if (/^(create_file|write_file|apply_patch|multi_edit|edit_file)$/.test(name) && t.path) paths.push(t.path);
    if (name === 'screenshot') { const m = /\(\/files\/([^)]+)\)/.exec(output || ''); if (m) paths.push(decodeURIComponent(m[1])); }
    paths.forEach((p) => {
      if (!PREVIEWABLE.test(extOf(p)) || artifacts.has(p)) return;
      const c = el('button', 'art', '<span class="ai">' + ic(/^(png|jpe?g|gif|webp|avif|svg)$/.test(extOf(p)) ? 'image' : /^(mp4|webm|mov|m4v)$/.test(extOf(p)) ? 'play' : 'file') + '</span><span class="at"><b></b><small></small></span>' + ic('chevR'));
      c.type = 'button'; c.dataset.path = p; c.querySelector('b').textContent = p.split('/').pop(); c.querySelector('small').textContent = kindOf(p) + ' \u00b7 Tap to open';
      artifacts.set(p, c);
      (g.nextSibling && g.nextSibling.classList && g.nextSibling.classList.contains('art') ? g.nextSibling : g).after(c);
    });
  }

  function renderTodos(todos) {
    const w = $('todoWrap');
    if (!todos || !todos.length) { w.classList.add('hidden'); w.innerHTML = ''; return; }
    const done = todos.filter((t) => t.status === 'done').length;
    w.classList.remove('hidden');
    w.innerHTML = '<details class="todos"' + (todoOpen ? ' open' : '') + '><summary>' + ic('list') + '<b>Tasks</b><span class="muted">' + done + ' of ' + todos.length + '</span>' + ic('chevD', 'chev') + '</summary><ul>' +
      todos.map((t) => '<li class="' + esc(t.status) + '"><span class="dot">' + (t.status === 'done' ? ic('check') : '') + '</span><span>' + esc(t.content) + '</span></li>').join('') + '</ul></details>';
    w.querySelector('details').ontoggle = (e) => { todoOpen = e.target.open; };
  }
  function renderConv(c) {
    $('title').textContent = c.title || 'New chat';
    ctxChars = JSON.stringify(c.messages || []).length; meter(); renderTodos(c.todos);
    if (!c.messages.length) return emptyState();
    msgs.innerHTML = ''; artifacts = new Map();
    const outputs = {}; c.messages.forEach((m) => { if (m.role === 'tool') outputs[m.tool_call_id] = m.content; });
    let grp = null, lastBot = null, lastBotText = '';
    c.messages.forEach((m) => {
      if (m.role === 'user') { addUser(m.content); grp = null; lastBot = null; if (!/^\[auto\]/.test(m.content)) lastUser = m.content; }
      else if (m.role === 'assistant') {
        if (m.content?.trim()) { lastBot = addBot(m.content, false); lastBot.classList.toggle('interim', !!m.tool_calls?.length); lastBotText = m.content; grp = null; }
        if (m.tool_calls?.length) {
          grp = grp || newGroup(); if (!grp.isConnected) msgs.append(grp);
          m.tool_calls.forEach((t) => toolRow(grp, t.id, t.function.name, t.function.arguments, outputs[t.id] ?? ''));
        }
      }
    });
    const last = c.messages[c.messages.length - 1];
    if (last && last.role === 'assistant' && !last.tool_calls?.length && lastBot) botActions(lastBot, lastBotText);
    stick = true; scroll(true);
  }

  // ---------- conversations ----------
  async function loadConvs() {
    const list = await api('/api/conversations'), nav = $('convs');
    nav.innerHTML = '';
    list.forEach((c) => {
      const row = el('div', 'conv' + (c.id === convId ? ' active' : ''));
      const a = el('a'); a.href = '#';
      if (c.open) { const b = el('span', 'badge'); b.textContent = c.open; a.append(b); }
      a.append(document.createTextNode(c.title || 'New chat'));
      a.onclick = (e) => { e.preventDefault(); openConv(c.id); closeSide(); };
      const del = el('button', null, ic('trash')); del.type = 'button'; del.title = 'Delete';
      del.onclick = async () => { if (!confirm('Delete this chat?')) return; await api('/api/conversations/' + c.id, { method: 'DELETE' }); if (c.id === convId) newChat(); loadConvs(); };
      row.append(a, del); nav.append(row);
    });
    return list;
  }
  async function openConv(id) {
    try { const c = await api('/api/conversations/' + id); convId = id; localStorage.setItem('pa_conv', id); renderConv(c); loadConvs(); }
    catch { newChat(); }
  }
  function newChat() { convId = null; localStorage.removeItem('pa_conv'); $('title').textContent = 'New chat'; attach = []; renderAttach(); renderTodos([]); ctxChars = 0; meter(); emptyState(); loadConvs(); closeSide(); if (!COARSE) $('input').focus(); }
  const closeSide = () => { $('side').classList.remove('open'); $('app').classList.remove('menu-open'); };
  const openSide = () => { $('side').classList.add('open'); $('app').classList.add('menu-open'); };

  // ---------- settings / models / modes ----------
  function paintMode() {
    const m = MODES.find((x) => x.value === curMode) || MODES[0];
    $('modeLbl').textContent = m.label; $('modeChip').dataset.mode = curMode;
  }
  async function checkSetup() {
    const s = await api('/api/settings');
    if (s.dashboardUrl) $('dash').href = s.dashboardUrl;
    const showFree = !!s.dashboardUrl && !s.hosted; // a hosting platform cannot reach FreeLLMAPI's local dashboard
    $('bnFree').classList.toggle('hidden', !showFree); $('bnExt').classList.toggle('hidden', showFree);
    $('banner').classList.toggle('hidden', s.hasKey);
    curMode = s.mode; paintMode();
    ctxBudget = s.tokenBudget || s.autoBudget || 200000; meter();
    $('whoSub').textContent = (MODES.find((x) => x.value === s.mode) || MODES[0]).label + ' \u00b7 ' + (s.maxSteps ? s.maxSteps + ' steps' : 'unlimited');
    return s;
  }
  async function loadModels(current) {
    try { models = (await api('/api/models')).models || []; } catch { models = []; }
    const want = current || localStorage.getItem('pa_model') || 'auto';
    curModel = want; $('modelLbl').textContent = curModel;
  }
  $('modeChip').onclick = () => openMenu($('modeChip'), MODES, curMode, (v) => { curMode = v; paintMode(); api('/api/settings', { method: 'POST', body: JSON.stringify({ mode: v }) }).then(checkSetup).catch(() => {}); });
  $('modelChip').onclick = () => {
    const list = ['auto', 'auto:fast'].concat(models.filter((m) => m !== 'auto' && m !== 'auto:fast')).map((m) => ({ value: m, label: m, desc: m === 'auto' ? 'Best available model, picked for you' : m === 'auto:fast' ? 'Fastest available model' : '' }));
    openMenu($('modelChip'), list, curModel, (v) => { curModel = v; $('modelLbl').textContent = v; localStorage.setItem('pa_model', v); api('/api/settings', { method: 'POST', body: JSON.stringify({ model: v }) }).catch(() => {}); });
  };

  // ---------- attachments ----------
  function renderAttach() {
    const box = $('attach'); box.classList.toggle('hidden', !attach.length); box.innerHTML = '';
    attach.forEach((a, i) => {
      const p = el('div', 'pill', ic('file') + '<span></span>'); p.querySelector('span').textContent = a.split('/').pop();
      const b = el('button', null, ic('x')); b.type = 'button'; b.onclick = () => { attach.splice(i, 1); renderAttach(); };
      p.append(b); box.append(p);
    });
  }
  async function uploadFiles(files) {
    for (const f of files) {
      try {
        const j = await api('/api/upload?name=' + encodeURIComponent(f.name || 'paste.png'), { method: 'POST', headers: { 'content-type': 'application/octet-stream' }, body: f });
        attach.push(j.path); renderAttach();
      } catch (x) { toast(f.name + ': ' + x.message); }
    }
  }

  // ---------- chat ----------
  function setBusy(b) { busy = b; $('send').classList.toggle('hidden', b); $('stop').classList.toggle('hidden', !b); }
  async function sendMsg(override) {
    const raw = (typeof override === 'string' ? override : $('input').value).trim();
    if ((!raw && !attach.length) || busy) return;
    const text = (typeof override === 'string' ? '' : (attach.length ? attach.map((a) => '@' + a).join(' ') + '\n\n' : '')) + raw;
    if (typeof override !== 'string') { $('input').value = ''; attach = []; renderAttach(); autosize(); }
    $('cmds').classList.add('hidden'); lastUser = text;
    if (!convId) { const c = await api('/api/conversations', { method: 'POST' }); convId = c.id; localStorage.setItem('pa_conv', convId); msgs.innerHTML = ''; }
    msgs.querySelector('.empty')?.remove();
    addUser(text); stick = true;
    const status = el('div', 'status', '<span class="sp">' + ic('spark') + '</span><span class="stx">Thinking</span><span class="el"></span>');
    msgs.append(status); scroll(true); setBusy(true);
    const t0 = Date.now(), tick = setInterval(() => { const s = Math.floor((Date.now() - t0) / 1000); status.querySelector('.el').textContent = s >= 4 ? s + 's' : ''; }, 1000);
    const setStatus = (t) => { status.querySelector('.stx').textContent = t; };
    const place = (node) => { msgs.insertBefore(node, status); scroll(); };
    let bot = null, grp = null; const live = new Map();
    try {
      const r = await fetch('/api/conversations/' + convId + '/chat', { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ message: text }) });
      if (!r.ok) throw new Error((await r.json().catch(() => ({}))).error || 'HTTP ' + r.status);
      const reader = r.body.getReader(), dec = new TextDecoder();
      let buf = '';
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buf += dec.decode(value, { stream: true });
        let idx;
        while ((idx = buf.indexOf('\n\n')) >= 0) {
          const chunk = buf.slice(0, idx); buf = buf.slice(idx + 2);
          const ev = (chunk.match(/^event: (.*)$/m) || [])[1], data = (chunk.match(/^data: (.*)$/m) || [])[1];
          if (!ev || !data) continue;
          let d; try { d = JSON.parse(data); } catch { continue; }
          if (ev === 'status') setStatus(d.text.replace(/\.{3}$/, ''));
          else if (ev === 'todos') renderTodos(d.todos);
          else if (ev === 'thought') {
            if (bot) { bot.classList.add('interim'); bot.innerHTML = md(d.content); enhance(bot); bot = null; }
            else { const b = el('div', 'bot interim', md(d.content)); place(b); enhance(b); }
          }
          else if (ev === 'delta') {
            if (!bot) { bot = el('div', 'bot'); place(bot); grp = null; }
            bot.dataset.raw = (bot.dataset.raw || '') + d.text;
            bot.innerHTML = md(bot.dataset.raw);
            ctxChars += d.text.length; meter(); scroll();
          }
          else if (ev === 'tool_start') {
            bot = null; if (!grp) { grp = newGroup(); place(grp); }
            live.set(d.id, { name: d.name, args: d.args });
            toolRow(grp, d.id, d.name, d.args, null); setStatus('Working'); scroll();
          }
          else if (ev === 'tool_end') { const l = live.get(d.id) || {}; finishTool(d.id, d.output, l.name, l.args); scroll(); }
          else if (ev === 'final') {
            let b = bot;
            if (b) { b.innerHTML = md(d.content); } else { b = el('div', 'bot', md(d.content)); place(b); ctxChars += d.content.length; }
            b.classList.remove('interim'); enhance(b); botActions(b, d.content); bot = null; meter(); scroll();
          }
          else if (ev === 'approval') {
            const box = el('div', 'appr', '<div><b>Allow</b> <code style="display:inline;padding:1px 6px;margin:0"></code>?</div><code class="ar"></code><div class="row"></div>');
            box.querySelector('code').textContent = d.tool; box.querySelector('.ar').textContent = typeof d.args === 'string' ? d.args : JSON.stringify(d.args, null, 2);
            const row = box.querySelector('.row');
            const yes = el('button', 'btn primary sm', 'Allow once'), always = el('button', 'btn sm', 'Always allow'), no = el('button', 'btn danger sm', 'Deny');
            [yes, always, no].forEach((b) => { b.type = 'button'; });
            const answer = async (allow, alw) => { await api('/api/approve/' + d.id, { method: 'POST', body: JSON.stringify({ allow, always: alw }) }); box.remove(); };
            yes.onclick = () => answer(true, false); always.onclick = () => answer(true, true); no.onclick = () => answer(false, false);
            row.append(yes, always, no); place(box);
          }
          else if (ev === 'error') { const e = el('div', 'errbox'); e.textContent = d.message; place(e); }
          else if (ev === 'done') { $('title').textContent = d.title; }
        }
      }
    } catch (e) { const x = el('div', 'errbox'); x.textContent = e.message; place(x); }
    clearInterval(tick); status.remove(); setBusy(false); loadConvs(); checkSetup().catch(() => {});
  }

  // ---------- artifact / preview panel ----------
  let pvPath = null, pvTab = 'prev';
  const setPvTabUi = (t) => { pvTab = t; $('pvTabPrev').classList.toggle('on', t === 'prev'); $('pvTabCode').classList.toggle('on', t === 'code'); };
  async function pvRender() {
    const p = pvPath, e = extOf(p), url = '/files/' + encPath(p);
    ['pvFrame', 'pvMedia', 'pvCode', 'pvNote'].forEach((i) => $(i).classList.add('hidden'));
    $('pvFrame').src = 'about:blank';
    const canPrev = PREVIEWABLE.test(e);
    $('pvTabPrev').classList.toggle('hidden', !canPrev);
    if (!canPrev && pvTab === 'prev') setPvTabUi('code');
    if (pvTab === 'code') {
      const pre = $('pvCode'); pre.textContent = 'Loading...'; pre.classList.remove('hidden');
      try { const j = await api('/api/fs/read?path=' + encPath(p)); pre.textContent = j.binary || j.image ? '(binary file)' : j.content; } catch (x) { pre.textContent = x.message; }
      return;
    }
    const media = $('pvMedia'); media.innerHTML = '';
    if (/^html?$/.test(e)) {
      try {
        const j = await api('/api/preview?path=' + encPath(p));
        $('pvFrame').src = j.up ? j.url : url + '?raw=1'; $('pvOpen').href = j.up ? j.url : url + '?raw=1';
        $('pvFrame').classList.remove('hidden');
        if (!j.up) { $('pvNote').textContent = 'Live preview server is off, so scripts are blocked here. Restart the agent or free the preview port.'; $('pvNote').classList.remove('hidden'); }
      } catch (x) { media.textContent = x.message; media.classList.remove('hidden'); }
    } else if (/^(svg|png|jpe?g|gif|webp|avif)$/.test(e)) { media.innerHTML = '<img alt="">'; media.firstChild.src = url + '?t=' + Date.now(); media.classList.remove('hidden'); }
    else if (/^(mp4|m4v|webm|mov)$/.test(e)) { media.innerHTML = '<video controls playsinline autoplay></video>'; media.firstChild.src = url; media.classList.remove('hidden'); }
    else if (/^(mp3|wav|ogg|m4a)$/.test(e)) { media.innerHTML = '<audio controls autoplay></audio>'; media.firstChild.src = url; media.classList.remove('hidden'); }
    else if (e === 'pdf') { $('pvFrame').src = url; $('pvFrame').classList.remove('hidden'); }
    else if (e === 'md') { try { const t = await (await fetch(url)).text(); media.innerHTML = '<div class="bot"></div>'; media.firstChild.innerHTML = md(t); media.classList.remove('hidden'); } catch (x) { media.textContent = x.message; media.classList.remove('hidden'); } }
  }
  function openPreview(p, tab) {
    pvPath = p; $('pvName').textContent = p.split('/').pop();
    $('pvOpen').href = '/files/' + encPath(p); $('pvDl').href = '/files/' + encPath(p) + '?download=1';
    $('pv').classList.remove('hidden'); $('fm').classList.add('hidden');
    setPvTabUi(tab || (PREVIEWABLE.test(extOf(p)) ? 'prev' : 'code')); pvRender();
  }
  const closePreview = () => { $('pv').classList.add('hidden'); $('pvFrame').src = 'about:blank'; $('pvMedia').innerHTML = ''; pvPath = null; };
  $('pvClose').onclick = closePreview; $('pvReload').onclick = () => pvPath && pvRender();
  $('pvTabPrev').onclick = () => { setPvTabUi('prev'); pvRender(); }; $('pvTabCode').onclick = () => { setPvTabUi('code'); pvRender(); };

  // ---------- file manager ----------
  let fmPath = '.', fmCur = null, fmDirty = false;
  const fmtSize = (n) => (n > 1048576 ? (n / 1048576).toFixed(1) + ' MB' : n > 1024 ? Math.round(n / 1024) + ' KB' : n + ' B');
  const fmShow = (on) => { $('fm').classList.toggle('hidden', !on); if (on) { closePreview(); fmList(fmPath); } };
  async function fmList(dir) {
    if (fmDirty && !confirm('Discard unsaved changes?')) return;
    fmPath = dir || '.'; fmCur = null; fmDirty = false; $('fmCrumb').textContent = fmPath === '.' ? '/' : '/' + fmPath;
    $('fmEditor').classList.add('hidden'); $('fmList').classList.remove('hidden');
    const box = $('fmList'); box.innerHTML = '<div class="fm-empty">Loading...</div>';
    try {
      const j = await api('/api/fs/list?path=' + encPath(fmPath));
      box.innerHTML = '';
      if (fmPath !== '.') {
        const up = el('div', 'fm-row', '<span class="ic">' + ic('arrowUp') + '</span><span class="nm">..</span>');
        up.onclick = () => fmList(fmPath.includes('/') ? fmPath.slice(0, fmPath.lastIndexOf('/')) : '.');
        box.append(up);
      }
      if (!j.entries.length) box.append(el('div', 'fm-empty', 'Empty folder'));
      j.entries.forEach((e) => {
        const rel = fmPath === '.' ? e.name : fmPath + '/' + e.name;
        const row = el('div', 'fm-row', '<span class="ic">' + ic(e.dir ? 'folder' : 'file') + '</span><span class="nm"></span><span class="sz">' + (e.dir ? '' : fmtSize(e.size)) +
          '</span><span class="ops"><button type="button" data-a="ren">' + ic('pencil') + '</button><button type="button" data-a="del">' + ic('trash') + '</button></span>');
        row.querySelector('.nm').textContent = e.name;
        row.onclick = (ev) => {
          const act = ev.target.closest('[data-a]')?.dataset.a;
          if (act === 'ren') return fmRename(rel, e.name);
          if (act === 'del') return fmDelete(rel, e.name);
          if (e.dir) fmList(rel); else fmOpenFile(rel);
        };
        box.append(row);
      });
    } catch (x) { box.innerHTML = ''; box.append(el('div', 'fm-empty', x.message)); }
  }
  async function fmOpenFile(rel) {
    if (fmDirty && !confirm('Discard unsaved changes?')) return;
    try {
      const j = await api('/api/fs/read?path=' + encPath(rel));
      fmCur = rel; fmDirty = false;
      $('fmList').classList.add('hidden'); $('fmEditor').classList.remove('hidden');
      $('fmName').textContent = rel; $('fmPrev').classList.toggle('hidden', !PREVIEWABLE.test(extOf(rel)));
      const ta = $('fmText'), pv = $('fmPreview');
      if (j.binary || j.image) {
        ta.classList.add('hidden'); pv.classList.remove('hidden'); pv.innerHTML = '';
        if (j.image) { const img = el('img'); img.src = '/files/' + encPath(rel); pv.append(img); } else pv.append(el('div', 'fm-empty', 'Binary file \u00b7 ' + fmtSize(j.size)));
      } else {
        pv.classList.add('hidden'); ta.classList.remove('hidden'); ta.value = j.content;
        if (j.size > 400000) $('fmName').textContent = rel + ' (truncated)';
      }
    } catch (x) { toast(x.message); }
  }
  async function fmSave() {
    if (!fmCur) return;
    try { await api('/api/fs/write', { method: 'POST', body: JSON.stringify({ path: fmCur, content: $('fmText').value }) }); fmDirty = false; $('fmSave').textContent = 'Saved'; setTimeout(() => ($('fmSave').textContent = 'Save'), 1200); }
    catch (x) { toast(x.message); }
  }
  async function fmRename(rel, name) {
    const next = prompt('Rename to:', name); if (!next || next === name) return;
    const dir = rel.includes('/') ? rel.slice(0, rel.lastIndexOf('/')) : '.';
    try { await api('/api/fs/rename', { method: 'POST', body: JSON.stringify({ from: rel, to: (dir === '.' ? '' : dir + '/') + next }) }); fmList(fmPath); } catch (x) { toast(x.message); }
  }
  async function fmDelete(rel, name) {
    if (!confirm('Delete "' + name + '"?')) return;
    try { await api('/api/fs/delete', { method: 'POST', body: JSON.stringify({ path: rel }) }); fmList(fmPath); } catch (x) { toast(x.message); }
  }
  async function fmNew(kind) {
    const name = prompt(kind === 'dir' ? 'New folder name:' : 'New file name:'); if (!name) return;
    const rel = (fmPath === '.' ? '' : fmPath + '/') + name;
    try {
      if (kind === 'dir') await api('/api/fs/mkdir', { method: 'POST', body: JSON.stringify({ path: rel }) });
      else await api('/api/fs/write', { method: 'POST', body: JSON.stringify({ path: rel, content: '' }) });
      await fmList(fmPath); if (kind === 'file') fmOpenFile(rel);
    } catch (x) { toast(x.message); }
  }
  async function fmUpload(files) {
    for (const f of files) {
      try { await api('/api/fs/upload?path=' + encPath(fmPath) + '&name=' + encodeURIComponent(f.name), { method: 'POST', headers: { 'content-type': 'application/octet-stream' }, body: f }); }
      catch (x) { toast(f.name + ': ' + x.message); }
    }
    fmList(fmPath);
  }

  // ---------- settings / memory ----------
  async function openSettings() {
    const s = await checkSetup(), f = $('setForm');
    f.apiKey.value = s.apiKey; f.baseUrl.value = s.baseUrl || ''; f.imageModel.value = s.imageModel;
    f.maxSteps.value = s.maxSteps; f.temperature.value = s.temperature; f.tokenBudget.value = s.tokenBudget;
    f.maxParallel.value = s.maxParallel; f.allowShell.checked = s.allowShell;
    f.fallbackModels.value = (s.fallbackModels || []).join(', '); f.persona.value = s.persona;
    $('setMsg').textContent = ''; $('setDlg').showModal();
  }
  async function saveSettings() {
    const f = $('setForm');
    await api('/api/settings', { method: 'POST', body: JSON.stringify({
      apiKey: f.apiKey.value.trim(), baseUrl: f.baseUrl.value.trim(), imageModel: f.imageModel.value.trim(),
      maxSteps: +f.maxSteps.value, temperature: +f.temperature.value, tokenBudget: +f.tokenBudget.value,
      maxParallel: +f.maxParallel.value, allowShell: f.allowShell.checked,
      fallbackModels: f.fallbackModels.value.split(',').map((x) => x.trim()).filter(Boolean), persona: f.persona.value,
    }) });
    $('setMsg').textContent = 'Saved'; checkSetup();
  }
  async function loadMems() {
    const list = await api('/api/memory'), ul = $('mems');
    ul.innerHTML = list.length ? '' : '<li><span class="muted">Nothing yet.</span></li>';
    list.forEach((m) => { const li = el('li'), sp = el('span'), b = el('button', null, ic('x')); b.type = 'button'; sp.textContent = m.text; b.onclick = async () => { await api('/api/memory/' + m.id, { method: 'DELETE' }); loadMems(); }; li.append(sp, b); ul.append(li); });
  }

  // ---------- theme ----------
  function paintTheme() {
    const t = document.documentElement.dataset.theme || (matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
    $('themeBtn').innerHTML = ic(t === 'dark' ? 'sun' : 'moon');
    document.querySelector('meta[name=theme-color]')?.setAttribute('content', t === 'dark' ? '#262624' : '#faf9f5');
  }
  $('themeBtn').onclick = () => {
    const cur = document.documentElement.dataset.theme || (matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
    const next = cur === 'dark' ? 'light' : 'dark'; document.documentElement.dataset.theme = next; localStorage.setItem('pa_theme', next); paintTheme();
  };
  paintTheme();

  // ---------- composer ----------
  const input = $('input');
  function autosize() { input.style.height = 'auto'; input.style.height = Math.min(220, input.scrollHeight) + 'px'; }
  function showCmds() {
    const v = input.value, box = $('cmds');
    if (!v.startsWith('/') || /\s/.test(v)) return box.classList.add('hidden');
    const hits = COMMANDS.filter(([c]) => c.startsWith(v.toLowerCase()));
    if (!hits.length) return box.classList.add('hidden');
    box.innerHTML = ''; box.classList.remove('hidden');
    hits.forEach(([c, d]) => { const b = el('button', null, '<b></b><span></span>'); b.type = 'button'; b.querySelector('b').textContent = c; b.querySelector('span').textContent = d; b.onclick = () => { input.value = c + ' '; box.classList.add('hidden'); autosize(); input.focus(); }; box.append(b); });
  }
  input.addEventListener('input', () => { autosize(); showCmds(); });
  input.addEventListener('keydown', (e) => {
    if (e.key === 'Tab' && input.value.startsWith('/')) { const hit = COMMANDS.find(([c]) => c.startsWith(input.value.trim())); if (hit) { e.preventDefault(); input.value = hit[0] + ' '; showCmds(); } }
    if (e.key === 'Enter' && !e.shiftKey && !e.isComposing && !COARSE) { e.preventDefault(); sendMsg(); }
    if (e.key === 'Escape' && busy && convId) api('/api/conversations/' + convId + '/stop', { method: 'POST' });
  });
  input.addEventListener('paste', (e) => { const files = [...(e.clipboardData?.files || [])]; if (files.length) { e.preventDefault(); uploadFiles(files); } });
  $('composer').onsubmit = (e) => { e.preventDefault(); sendMsg(); };
  $('stop').onclick = () => convId && api('/api/conversations/' + convId + '/stop', { method: 'POST' });
  $('clip').onclick = () => $('filePick').click();
  $('filePick').onchange = (e) => { if (e.target.files.length) uploadFiles([...e.target.files]); e.target.value = ''; };
  $('newChat').onclick = newChat;
  $('openSet').onclick = openSettings; $('bannerSet').onclick = openSettings; $('bannerSet2').onclick = openSettings;
  $('saveSet').onclick = () => saveSettings().catch((e) => ($('setMsg').textContent = e.message));
  $('openMem').onclick = () => { closeSide(); loadMems(); $('memDlg').showModal(); };
  $('memAdd').onclick = async () => { const v = $('memIn').value.trim(); if (!v) return; await api('/api/memory', { method: 'POST', body: JSON.stringify({ text: v }) }); $('memIn').value = ''; loadMems(); };
  $('logout').onclick = async () => { await api('/api/logout', { method: 'POST' }).catch(() => {}); showLogin(); };
  $('menu').onclick = openSide; $('sideClose').onclick = closeSide; $('scrim').onclick = closeSide;
  msgs.addEventListener('click', (e) => {
    const t = e.target;
    const cp = t.closest('.copy'); if (cp) { navigator.clipboard?.writeText(cp.closest('pre').querySelector('code').textContent); cp.textContent = 'Copied'; setTimeout(() => (cp.textContent = 'Copy'), 1200); return; }
    const art = t.closest('.art, a.artlink'); if (art && art.dataset.path) { e.preventDefault(); openPreview(art.dataset.path); }
  });
  $('openFiles').onclick = () => { closeSide(); fmShow(true); }; $('filesBtn').onclick = () => fmShow(true);
  $('fmClose').onclick = () => { if (!fmDirty || confirm('Discard unsaved changes?')) { fmDirty = false; fmShow(false); } };
  $('fmNewFile').onclick = () => fmNew('file'); $('fmNewDir').onclick = () => fmNew('dir');
  $('fmUpload').onclick = () => $('fmFile').click();
  $('fmFile').onchange = (e) => { if (e.target.files.length) fmUpload([...e.target.files]); e.target.value = ''; };
  $('fmSave').onclick = fmSave; $('fmBack').onclick = () => fmList(fmPath);
  $('fmPrev').onclick = () => fmCur && openPreview(fmCur);
  $('fmDl').onclick = () => fmCur && window.open('/files/' + encPath(fmCur) + '?download=1');
  $('fmAsk').onclick = () => { if (!fmCur) return; attach.push(fmCur); renderAttach(); fmShow(false); input.focus(); autosize(); };
  $('fmText').addEventListener('input', () => { fmDirty = true; });
  $('fmText').addEventListener('keydown', (e) => { if ((e.ctrlKey || e.metaKey) && e.key === 's') { e.preventDefault(); fmSave(); } });
  document.addEventListener('keydown', (e) => {
    if (e.ctrlKey && e.shiftKey && e.key.toLowerCase() === 'f') { e.preventDefault(); fmShow(true); }
    if (e.key === 'Escape') { if (!$('pop').classList.contains('hidden')) closeMenu(); else if (!$('pv').classList.contains('hidden')) closePreview(); else if (!$('fm').classList.contains('hidden') && document.activeElement !== $('fmText')) fmShow(false); }
  });
  ['dragover', 'dragenter'].forEach((t) => document.addEventListener(t, (e) => { e.preventDefault(); document.body.classList.add('dropping'); }));
  ['dragleave', 'drop'].forEach((t) => document.addEventListener(t, (e) => { e.preventDefault(); if (t === 'dragleave' && e.relatedTarget) return; document.body.classList.remove('dropping'); }));
  document.addEventListener('drop', (e) => { const files = [...(e.dataTransfer?.files || [])]; if (files.length) uploadFiles(files); });
  window.addEventListener('beforeunload', (e) => { if (fmDirty) { e.preventDefault(); e.returnValue = ''; } });

  function showLogin() { $('app').classList.add('hidden'); $('login').classList.remove('hidden'); $('pw').focus(); }
  async function showApp() {
    $('login').classList.add('hidden'); $('app').classList.remove('hidden');
    const s = await checkSetup();
    await loadModels(s.model);
    const list = await loadConvs();
    if (convId && list.some((c) => c.id === convId)) openConv(convId); else newChat();
  }
  $('loginForm').onsubmit = async (e) => {
    e.preventDefault(); $('loginErr').textContent = '';
    try { await api('/api/login', { method: 'POST', body: JSON.stringify({ password: $('pw').value }) }); $('pw').value = ''; showApp(); }
    catch (x) { $('loginErr').textContent = x.message; }
  };
  api('/api/me').then((m) => (m.authed ? showApp() : showLogin())).catch(showLogin);
}

const PAGE = `<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover,interactive-widget=resizes-content">
<meta name="theme-color" content="#262624"><meta name="color-scheme" content="dark light">
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 24 24'%3E%3Cpath d='M12 2.4v19.2M3.7 7.2l16.6 9.6M3.7 16.8L20.3 7.2M12 2.4v19.2' stroke='%23d97757' stroke-width='2.7' stroke-linecap='round' fill='none'/%3E%3C/svg%3E">
<title>Power Agent</title>
<script>try{var t=localStorage.getItem('pa_theme');if(t)document.documentElement.dataset.theme=t}catch(e){}</script>
<style>${CSS}</style></head><body>${BODY}<script>(${clientApp.toString()})();</script></body></html>`;


// ============================================================================
// 18. MAIN
// ============================================================================
async function shutdown() {
  if (shuttingDown) return;
  shuttingDown = true;
  log('Shutting down...');
  for (const [, srv] of mcpServers) { try { srv.child.kill(); } catch { /* ignore */ } }
  await freellm.stop();
  process.exit(0);
}
process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
process.on('unhandledRejection', (e) => err(`unhandled rejection: ${(e && e.stack) || e}`));

const needGit = () => git(['--version']).catch(() => { throw new Error('git is not installed / not on PATH. Install git, or skip FreeLLMAPI by setting LLM_BASE_URL + LLM_API_KEY (any OpenAI-compatible API).'); });

// FreeLLMAPI (clone / build / run) happens in the background AFTER the web server is already listening
async function bootBackend() {
  try {
    if (!USE_FREELLM) { setBoot('ready', EXTERNAL_LLM ? `Using ${LLM_BASE}` : 'FreeLLMAPI disabled'); return; }
    setBoot('starting', 'installing / building FreeLLMAPI, first run takes a few minutes.');
    await needGit();
    await ensureClone();
    syncFreeLLMEnv();
    await checkForUpdate({ restart: false });
    await installAndBuild();
    setBoot('starting', 'starting FreeLLMAPI.');
    log('Starting FreeLLMAPI...');
    await freellm.start();
    setBoot('ready', 'FreeLLMAPI is running');
    log(`${K.g}FreeLLMAPI is ready${K.x}: ${FREELLM_URL}`);
    if (CFG.updateMinutes > 0) setInterval(() => checkForUpdate(), CFG.updateMinutes * 60000);
  } catch (e) {
    setBoot('error', String(e.message || e).split('\n')[0]);
    err(`FreeLLMAPI could not start: ${e.stack || e.message}`);
  }
}

async function main() {
  if (Number(process.versions.node.split('.')[0]) < 20) throw new Error(`Node.js 20+ required (you have ${process.versions.node}).`);
  log(`Home: ${HOME}${PLATFORM ? `  (${PLATFORM}${HOSTED ? ', hosted mode' : ''})` : ''}`);
  if (process.argv.includes('--update-only')) {
    if (!USE_FREELLM) { log('Nothing to update: FreeLLMAPI is not used.'); process.exit(0); }
    await needGit(); await ensureClone(); syncFreeLLMEnv(); await checkForUpdate({ restart: false }); await installAndBuild();
    log('Update-only run done.'); process.exit(0);
  }

  await startAgentServer();   // listen FIRST: hosting platforms kill apps that do not open their port quickly
  await startPreviewServer();
  const backend = bootBackend();
  const mcp = connectMcp().then(() => { if (mcpServers.size) log(`MCP: ${mcpServers.size} server(s) connected`); }).catch((e) => err(`MCP: ${e.message}`));
  sysProbe(true);

  const agentUrl = CFG.publicUrl || localUrl(CFG.agentHost, CFG.agentPort);
  const pw = CFG.pwFromEnv ? '(the AGENT_PASSWORD environment variable)' : `${K.y}${CFG.password}${K.x}`;
  console.log(`
${K.g}Power Agent v4 is running${K.x}${PLATFORM ? `  [${PLATFORM}]` : ''}
  Power Agent : ${agentUrl}   (listening on ${CFG.agentHost}:${CFG.agentPort})
     -> password: ${pw}   then ${USE_FREELLM ? 'paste the unified key' : 'paste your API key'} in Settings
  LLM         : ${USE_FREELLM ? `FreeLLMAPI, starting in the background (dashboard ${FREELLM_URL})` : EXTERNAL_LLM ? `${LLM_BASE}${ENV.LLM_MODEL ? `  model: ${ENV.LLM_MODEL}` : ''}` : 'not configured yet: set the API base URL in Settings (or LLM_BASE_URL on the server)'}
  Workspace: ${WORKSPACE}
  Live preview: ${CFG.previewMode === 'inline' ? 'inline (same port, sandboxed)' : previewUp ? localUrl(CFG.agentHost, CFG.previewPort) : 'off'}   Browser: ${findChrome() || 'none yet (the agent installs one when needed)'}
  MCP: ${Object.keys(readJson(MCP_FILE, { servers: {} }).servers || {}).length} configured (connecting in background)   Auto-update: ${USE_FREELLM && CFG.updateMinutes > 0 ? `every ${CFG.updateMinutes} min` : 'off'}
  Mode: ${getSettings().mode}   Steps: ${getSettings().maxSteps || 'unlimited'}   Ctrl+C to stop.
`);
  if (HOSTED) {
    warn('This agent can run shell commands. On a public URL the password is the only lock: use a long one.');
    if (!CFG.pwFromEnv) warn(`AGENT_PASSWORD is not set as an environment variable. The generated one lives in ${ENV_FILE}; on a disk that is wiped on redeploy it changes every time. Set AGENT_PASSWORD (and POWER_AGENT_HOME on a persistent volume).`);
    else if (String(CFG.password).length < 12) warn('AGENT_PASSWORD is shorter than 12 characters.');
    if (USE_FREELLM) warn(`FreeLLMAPI's dashboard only listens on ${FREELLM_URL}, which a hosting platform does not expose, so you could not add provider keys. Easiest fix: set LLM_BASE_URL + LLM_API_KEY + LLM_MODEL to use OpenRouter / Groq / OpenAI / any OpenAI-compatible API (skips the slow FreeLLMAPI build too).`);
  }
  if (CFG.selfPingMinutes > 0) {
    if (!CFG.publicUrl) warn('SELF_PING_MINUTES needs PUBLIC_URL (the address the internet uses), otherwise the ping cannot keep the host awake.');
    else {
      const target = `${CFG.publicUrl}/health`;
      setInterval(() => fetch(target, { signal: AbortSignal.timeout(15000) }).catch(() => {}), CFG.selfPingMinutes * 60000);
      log(`Keep-alive ping every ${CFG.selfPingMinutes} min -> ${target}`);
    }
  }
  await Promise.all([backend, mcp]);
}
main().catch(async (e) => { err(e.stack || e.message); await freellm.stop(); process.exit(1); });
})().catch((e) => { console.error(e); process.exit(1); });
