/* shapeshift serve — console client.
 *
 * Vanilla ES2020, no framework, no bundler, no dependency. Talks to the same-origin
 * JSON API (/api/*) the hand-rolled Rust server exposes. Kept lean on purpose: this
 * is a shaper console, not a control plane. */

"use strict";

const $ = (id) => document.getElementById(id);

// ---- tiny helpers -----------------------------------------------------------

function toast(msg, kind) {
  const t = document.createElement("div");
  t.className = "toast" + (kind ? " " + kind : "");
  t.textContent = msg;
  $("toasts").appendChild(t);
  setTimeout(() => t.remove(), 4200);
}

async function api(path, body) {
  const res = await fetch(path, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body || {}),
  });
  let data = null;
  try { data = await res.json(); } catch (_) { /* non-JSON */ }
  if (!res.ok) {
    const msg = (data && data.error) || `request failed (${res.status})`;
    throw new Error(msg);
  }
  return data;
}

function fmtNum(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return (n / 1e9).toFixed(1) + "B";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + "M";
  return n.toLocaleString("en-US");
}
function fmtBytes(n) {
  n = Number(n) || 0;
  if (n >= 1e9) return (n / 1e9).toFixed(1) + " GB";
  if (n >= 1e6) return (n / 1e6).toFixed(1) + " MB";
  if (n >= 1e3) return (n / 1e3).toFixed(1) + " KB";
  return n + " B";
}
function esc(s) {
  return String(s == null ? "" : s)
    .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
}
function baseName(p) {
  const parts = String(p || "").split("/");
  return parts[parts.length - 1] || p;
}

// ---- sample data ------------------------------------------------------------

const SAMPLE = `{"id":1,"user":{"name":"Ada","plan":"pro"},"amount":12.50,"active":true,"day":"2026-07-01","event_at":"2026-07-01T10:00:00Z","tags":["a","b"]}
{"id":2,"user":{"name":"Grace","plan":"free"},"amount":3.00,"active":false,"day":"2026-07-02","event_at":"2026-07-02T09:30:00Z","tags":[]}
{"id":3,"user":{"name":"Linus","plan":"team"},"amount":99.99,"active":true,"day":"2026-07-02","event_at":"2026-07-02T14:12:00Z","tags":["x"]}
{"id":4,"user":{"name":"Katherine","plan":"pro"},"amount":0.51,"active":true,"day":"2026-07-03","event_at":"2026-07-03T08:05:00Z","tags":["y","z"]}
not-json-a-bad-line`;

// ---- navigation -------------------------------------------------------------

const TITLES = {
  overview: ["Overview", "What needs a look, and what ran."],
  catalog: ["Catalog", "A REST catalog's namespaces and tables, read-only: each table's history, and how to read it from an engine."],
  connectors: ["Connectors", "Ingest URLs producers push JSON to, each with its own token; batches land through a dataset."],
  alerts: ["Alerts", "Rules over what needs a look, and the webhooks they tell: once when an incident opens, once when it resolves."],
  datasets: ["Datasets", "Specs with a name: what each reads, where it lands, and how fresh it should stay. Run them here."],
  shape: ["Shape", "Infer a spec, shape JSON into Parquet or Iceberg, and see what landed."],
  runs: ["Runs", "Every run this console started: its phases, how it came out, and what it committed."],
  tables: ["Tables", "A table's history, read from its Iceberg metadata: every snapshot as an event."],
  inspect: ["Inspect", "Read back a Parquet file or an Iceberg table — schema and row count."],
};

function showView(name) {
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.toggle("active", n.dataset.view === name));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === "view-" + name));
  const [t, s] = TITLES[name] || TITLES.overview;
  $("view-title").textContent = t;
  $("view-sub").textContent = s;
  // The runs list follows runs as they go while it is on screen.
  clearInterval(runsState.timer);
  runsState.timer = null;
  if (name === "runs") {
    loadRuns();
    runsState.timer = setInterval(loadRuns, 2000);
  }
  if (name === "tables") loadTables();
  // The overview follows what happens while it is on screen.
  clearInterval(ovState.timer);
  ovState.timer = null;
  if (name === "overview") {
    loadOverview();
    ovState.timer = setInterval(loadOverview, 5000);
  }
  if (name === "datasets") loadDatasets();
  if (name === "alerts") { loadDatasetsQuiet(); loadAlerts(); }
  if (name === "catalog") loadCatalog();
  clearInterval(cnState.timer);
  cnState.timer = null;
  if (name === "connectors") {
    loadDatasetsQuiet();
    loadConnectors();
    cnState.timer = setInterval(loadConnectors, 3000);
  }
}

// ---- infer ------------------------------------------------------------------

async function doInfer() {
  const input = $("src-input").value;
  if (!input.trim()) { toast("Paste some records first.", "err"); return; }
  const btn = $("btn-infer");
  btn.disabled = true; btn.textContent = "Inferring…";
  try {
    const res = await api("/api/infer", {
      input,
      format: $("src-format").value,
      dataset: $("out-dataset").value,
    });
    $("spec-yaml").value = res.spec_yaml;
    syncSpecToControls();
    const note = $("infer-note");
    note.style.display = "";
    note.className = "badge good";
    note.textContent = `${res.columns.length} columns · sampled ${res.sampled}`;
    toast(`Inferred ${res.columns.length} columns from ${res.sampled} records.`, "ok");
  } catch (e) {
    toast(e.message, "err");
  } finally {
    btn.disabled = false; btn.textContent = "Infer spec from sample";
  }
}

// ---- shape ------------------------------------------------------------------

async function doShape() {
  const input = $("src-input").value;
  if (!input.trim()) { toast("Paste some records first.", "err"); return; }
  const btn = $("btn-shape");
  btn.disabled = true; btn.textContent = "Shaping…";
  try {
    const res = await api("/api/shape", {
      input,
      spec_yaml: $("spec-yaml").value,
      format: $("src-format").value,
      to: $("out-format").value,
      compression: $("out-compression").value,
      dataset: $("out-dataset").value,
      partition_by: partitionList(),
      append: $("out-append").checked,
      on_drift: $("out-drift").value,
    });
    renderShape(res);
    toast(`${res.run_id}: shaped ${fmtNum(res.rows_out)} rows → ${baseName(res.output_path)}`, "ok");
    loadRuns();
  } catch (e) {
    toast(e.message, "err");
    $("result-hint").textContent = "failed";
  } finally {
    btn.disabled = false; btn.textContent = "Shape";
  }
}

function partitionList() {
  const raw = $("out-partition").value.trim();
  if (!raw || $("out-format").value !== "iceberg") return [];
  return [raw]; // server splits on top-level commas
}

function renderShape(res) {
  $("result-hint").textContent = `${res.run_id} · ${res.format} · ${fmtBytes(res.bytes)}`;
  const rejTone = res.rejected > 0 ? "warn" : "good";
  const parseTone = res.parse_errors > 0 ? "warn" : "good";
  const shapeTone = (res.rejected > 0 || res.parse_errors > 0) ? "warn" : "good";

  const stats = `
    <div class="stats">
      <div class="stat"><div class="k">rows in</div><div class="v">${fmtNum(res.rows_in)}</div></div>
      <div class="stat"><div class="k">rows out</div><div class="v good">${fmtNum(res.rows_out)}</div></div>
      <div class="stat"><div class="k">row groups</div><div class="v">${fmtNum(res.row_groups)}</div></div>
      <div class="stat"><div class="k">rejected</div><div class="v ${res.rejected > 0 ? "warn" : ""}">${fmtNum(res.rejected)}</div></div>
      <div class="stat"><div class="k">parse errors</div><div class="v ${res.parse_errors > 0 ? "warn" : ""}">${fmtNum(res.parse_errors)}</div></div>
      <div class="stat"><div class="k">bytes</div><div class="v">${fmtBytes(res.bytes)}</div></div>
    </div>`;

  const pipeline = `
    <div>
      <div class="section-title">Pipeline</div>
      <div class="pipeline">
        <div class="pstep good"><div class="ph"><span class="dot"></span><span class="pn">capture</span></div><div class="pd">read ${fmtNum(res.rows_in)} records</div></div>
        <div class="parrow"></div>
        <div class="pstep ${shapeTone}"><div class="ph"><span class="dot"></span><span class="pn">shape</span></div><div class="pd">${fmtNum(res.rows_out)} out · ${fmtNum(res.rejected)} rejected · ${fmtNum(res.parse_errors)} parse err</div></div>
        <div class="parrow"></div>
        <div class="pstep good"><div class="ph"><span class="dot"></span><span class="pn">land</span></div><div class="pd">${esc(res.format)} → ${esc(baseName(res.output_path))}</div></div>
      </div>
    </div>`;

  const output = `
    <div>
      <div class="section-title">Output</div>
      <div class="codeline">${esc(res.output_path)}</div>
      ${res.metadata_path ? `<div class="codeline" style="margin-top:6px">${esc(res.metadata_path)}</div>` : ""}
      <div class="btn-row" style="margin-top:10px">
        <button class="btn sm primary" id="btn-inspect-out">Inspect output →</button>
        ${res.rejects_path ? `<span class="badge warn">${fmtNum(res.rejects_total)} rejected → ${esc(baseName(res.rejects_path))}</span>` : ""}
      </div>
    </div>`;

  let rejects = "";
  if (res.rejects_sample && res.rejects_sample.length) {
    const rows = res.rejects_sample.map((r) => {
      const rawStr = typeof r.raw === "string" ? r.raw : JSON.stringify(r.raw);
      const lineTag = r.line != null ? `line ${r.line}` : "shaped-out";
      return `<div class="reject"><div class="rh"><span class="rl">${esc(lineTag)}</span><span class="re">${esc(r.error)}</span></div><div class="rr">${esc(rawStr)}</div></div>`;
    }).join("");
    const more = res.rejects_total > res.rejects_sample.length
      ? `<div class="note" style="font-size:11.5px;color:var(--eg-faint);margin-top:6px">Showing ${res.rejects_sample.length} of ${fmtNum(res.rejects_total)} — full set in the sidecar.</div>` : "";
    rejects = `<div><div class="section-title">Rejected rows</div><div class="rejects">${rows}</div>${more}</div>`;
  }

  let drift = "";
  if (res.drift) {
    const d = res.drift;
    const row = (r) => {
      const what = r.declared_type
        ? `column <code>${esc(r.path)}</code> (${esc(r.declared_type)}) → now looks like ${esc(r.suggested_type)}`
        : `new field <code>${esc(r.path)}</code> → suggest ${esc(r.suggested_type)}`;
      // Absent means "no example was kept" — a JSON null would be a real observed
      // value, so only `undefined` is suppressed.
      const eg = r.example === undefined
        ? "" : ` · e.g. ${esc(JSON.stringify(r.example))}`;
      return `<div class="reject"><div class="rh"><span class="rl">${fmtNum(r.count)} row(s)</span><span class="re">first at record ${fmtNum(r.first_record)}</span></div><div class="rr">${what}${eg}</div></div>`;
    };
    const rows = [...d.new_fields, ...d.type_mismatches].map(row).join("");
    const shown = d.new_fields.length + d.type_mismatches.length;
    const total = d.new_fields_total + d.type_mismatches_total;
    // When the engine hit its path cap, `total` is what was tracked — not what drifted.
    const capped = d.truncated
      ? " The run hit its tracked-path cap, so further paths were not counted at all."
      : "";
    const more = (total > shown || d.truncated)
      ? `<div class="note" style="font-size:11.5px;color:var(--eg-faint);margin-top:6px">Showing ${shown} of ${fmtNum(total)} ${d.truncated ? "tracked" : "drifted"} paths.${capped}</div>` : "";
    // What the policy actually did to the rows, so a drift report is never mistaken
    // for "rows were dropped".
    const did = d.policy === "quarantine"
      ? `${fmtNum(d.rows_quarantined)} row(s) quarantined — not written`
      : d.policy === "rescue"
        ? `${fmtNum(d.rows_rescued)} row(s) rescued into the catch-all column`
        : "every row was written as-is; nothing was dropped for drift";
    const suggest = d.suggested_columns_yaml
      ? `<div class="note" style="margin-top:8px">Add to the spec's <code>columns:</code></div><pre class="codeline" style="white-space:pre-wrap;margin-top:6px">${esc(d.suggested_columns_yaml)}</pre>`
      : "";
    drift = `<div><div class="section-title">Schema drift <span class="badge warn">policy: ${esc(d.policy)}</span></div>
      <div class="note" style="font-size:11.5px;color:var(--eg-faint);margin-bottom:6px">${fmtNum(d.rows_with_drift)} of ${fmtNum(d.rows_scanned)} rows drifted · ${did}</div>
      <div class="rejects">${rows}</div>${more}${suggest}</div>`;
  }

  $("shape-result").innerHTML = stats + pipeline + output + drift + rejects;
  const ob = $("btn-inspect-out");
  if (ob) ob.addEventListener("click", () => { $("inspect-path").value = res.output_path; showView("inspect"); doInspect(); });
}

// ---- inspect ----------------------------------------------------------------

async function doInspect() {
  const path = $("inspect-path").value.trim();
  if (!path) { toast("Enter a path to inspect.", "err"); return; }
  const btn = $("btn-inspect");
  btn.disabled = true; btn.textContent = "Inspecting…";
  try {
    const res = await api("/api/inspect", { path });
    renderInspect(res);
  } catch (e) {
    toast(e.message, "err");
    $("inspect-result").innerHTML = "";
  } finally {
    btn.disabled = false; btn.textContent = "Inspect";
  }
}

function renderInspect(res) {
  const colRows = (res.columns || []).map(
    (c) => `<tr><td class="mono">${esc(c.name)}</td><td class="mono">${esc(c.type)}</td></tr>`
  ).join("");
  const colTable = `<div class="tbl-wrap"><table class="tbl"><thead><tr><th>Column</th><th>Type</th></tr></thead><tbody>${colRows}</tbody></table></div>`;

  let head;
  if (res.kind === "iceberg") {
    head = `
      <div class="btn-row" style="margin-bottom:12px"><span class="badge brand">Iceberg v${res.format_version}</span><span class="badge">${(res.columns || []).length} columns</span></div>
      <div class="stats" style="grid-template-columns:repeat(2,1fr);margin-bottom:14px">
        <div class="stat"><div class="k">total records</div><div class="v">${fmtNum(res.total_records)}</div></div>
        <div class="stat"><div class="k">current snapshot</div><div class="v" style="font-size:13px">${esc(res.current_snapshot_id)}</div></div>
        <div class="stat" style="grid-column:1/-1"><div class="k">table uuid</div><div class="v" style="font-size:12px">${esc(res.table_uuid)}</div></div>
      </div>`;
  } else {
    head = `
      <div class="btn-row" style="margin-bottom:12px"><span class="badge brand">Parquet</span><span class="badge">${(res.columns || []).length} columns</span></div>
      <div class="stats" style="grid-template-columns:repeat(2,1fr);margin-bottom:14px">
        <div class="stat"><div class="k">rows</div><div class="v">${fmtNum(res.rows)}</div></div>
        <div class="stat"><div class="k">row groups</div><div class="v">${fmtNum(res.row_groups)}</div></div>
      </div>`;
  }
  $("inspect-result").innerHTML = `<div style="margin-top:16px">${head}${colTable}<div class="codeline" style="margin-top:12px">${esc(res.path)}</div></div>`;
}

// ---- runs -------------------------------------------------------------------

const OUTCOME = {
  running: ["", "running"],
  ok: ["✓", "committed"],
  earlier: ["✓", "already committed"],
  conflict: ["75", "conflict · 75"],
  failed: ["✕", "failed · 1"],
  interrupted: ["■", "interrupted"],
};
const runsState = { runs: [], sel: null, timer: null };

function fmtDur(ms) {
  ms = Math.max(0, Math.round(ms));
  if (ms < 1000) return ms + " ms";
  const s = ms / 1000;
  if (s < 60) return s.toFixed(s < 10 ? 2 : 1) + " s";
  const m = Math.floor(s / 60);
  if (m < 60) return m + " min " + Math.round(s % 60) + " s";
  return Math.floor(m / 60) + " h " + (m % 60) + " min";
}
function fmtTime(ms) {
  return new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit", second: "2-digit" });
}
function fmtAgo(ms) {
  const s = Math.max(0, Math.round((Date.now() - ms) / 1000));
  if (s < 60) return s + " s ago";
  const m = Math.floor(s / 60);
  if (m < 60) return m + " min ago";
  const h = Math.floor(m / 60);
  return h < 48 ? h + " h ago" : Math.floor(h / 24) + " d ago";
}
function pill(run) {
  const o = OUTCOME[run.outcome] || ["?", run.outcome];
  return `<span class="pill o-${esc(run.outcome)}"><span class="g">${o[0]}</span>${esc(o[1])}</span>`;
}

// A run's phases on its own clock, start to end (or now), with its outcome at the end.
function runTrack(run) {
  const start = run.started_ms;
  const end = run.ended_ms || Date.now();
  const span = Math.max(end - start, 1);
  const X = (t) => ((t - start) / span) * 100;
  let h = "";
  for (const p of run.phases || []) {
    const a = p.start_ms, b = p.end_ms || Date.now();
    const L = X(a), W = Math.max(X(b) - L, 0.8);
    h += `<span class="seg seg-${esc(p.name)}" style="left:${L.toFixed(2)}%;width:${W.toFixed(2)}%" title="${esc(p.name)} · ${fmtDur(b - a)}"></span>`;
  }
  // The outcome column says how a finished run came out; a running one pulses at its edge.
  if (run.outcome === "running") {
    h += `<span class="cap running" style="left:100%" title="running"></span>`;
  }
  return `<span class="track">${h}</span>`;
}

// A GET that fails (an error status too) throws, so a failed refresh never replaces what is
// shown with nothing.
async function getJson(path) {
  const res = await fetch(path);
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error((data && data.error) || `request failed (${res.status})`);
  return data;
}

async function loadRuns() {
  try {
    const data = await getJson("/api/runs");
    runsState.runs = data.runs || [];
    renderRuns();
  } catch (_) { /* the health pill says when the server is gone; the list stays as it was */ }
}

function renderRuns() {
  const runs = runsState.runs;
  const running = runs.filter((r) => r.outcome === "running").length;
  const count = $("runs-count");
  count.classList.toggle("hidden", running === 0);
  count.textContent = running;
  const list = $("runs-list");
  if (!runs.length) {
    list.innerHTML = `<div class="empty"><div class="et">No runs yet</div>
      Shape something and it shows here: its phases, how it came out, and the snapshot it committed.
      Runs are kept in this console's data directory.</div>`;
    $("run-detail").innerHTML = "";
    return;
  }
  if (!runsState.sel || !runs.some((r) => r.id === runsState.sel)) runsState.sel = runs[0].id;
  list.innerHTML = `<div class="tl">${runs.map((r) => `
    <div class="tl-row${r.outcome === "running" ? "" : " done"}${r.id === runsState.sel ? " sel" : ""}" data-run="${esc(r.id)}">
      <div class="tl-meta">
        <span class="tl-id">${esc(r.id)}</span>
        <span class="tl-ds">${esc(r.dataset || "")} → ${esc(r.output || r.format || "")}</span>
        <span class="tl-when">${fmtAgo(r.started_ms)} · ${r.ended_ms ? "took " + fmtDur(r.ended_ms - r.started_ms) : "running for " + fmtDur(Date.now() - r.started_ms)}</span>
      </div>
      ${runTrack(r)}
      <div>${pill(r)}</div>
    </div>`).join("")}</div>`;
  renderRunDetail();
}

function renderRunDetail() {
  const r = runsState.runs.find((x) => x.id === runsState.sel);
  const box = $("run-detail");
  if (!r) { box.innerHTML = ""; return; }
  const sm = r.summary || {};
  const lines = [[r.started_ms, "started", `shape ${r.dataset || ""}${r.output ? " → " + r.output : ""}`, ""]];
  for (const p of r.phases || []) {
    if (p.start_ms !== r.started_ms) lines.push([p.start_ms, p.name, "", ""]);
  }
  if (r.ended_ms) {
    const tone = r.outcome === "ok" || r.outcome === "earlier" ? "out" : r.outcome === "conflict" ? "in" : "bad";
    const what = r.error ? `${r.error.kind} · ${r.error.message}`
      : sm.snapshot_id ? `snapshot ${sm.snapshot_id}${sm.table_version ? " · v" + sm.table_version : ""}` : "file written";
    lines.push([r.ended_ms, `exit ${r.exit}`, what, tone]);
  }
  const kv = (k, v) => `<dt>${k}</dt><dd>${v}</dd>`;
  let facts = kv("output", esc(r.output || "—")) + kv("format", esc(r.format || "—"))
    + kv("started", fmtTime(r.started_ms))
    + kv("took", r.ended_ms ? fmtDur(r.ended_ms - r.started_ms) : "running");
  if (r.merge_on) facts += kv("merge on", esc(r.merge_on.join(", ")));
  if (r.summary) {
    facts += kv("rows", `${fmtNum(sm.rows_in)} in · ${fmtNum(sm.rows_out)} out`)
      + kv("rejected", `${fmtNum((sm.rejected || 0) + (sm.parse_errors || 0))}${sm.rejects_path ? ` <span class="t2">→ ${esc(sm.rejects_path)}</span>` : ""}`)
      + kv("written", `${fmtBytes(sm.bytes)} <span class="t2">· ${fmtNum(sm.row_groups)} row groups</span>`);
    if (sm.snapshot_id) {
      facts += kv("snapshot", `<a class="lnk" data-open-table="${esc(r.output)}" data-snap="${esc(sm.snapshot_id)}">${esc(sm.snapshot_id)}</a>`);
    }
  }
  const caveat = r.outcome === "conflict"
    ? `<div class="caveat"><b>Exit 75: the table moved under this run</b>Another writer committed first. Nothing of this run was published and its files were removed; it is safe to run again.</div>`
    : r.outcome === "failed" || r.outcome === "interrupted"
      ? `<div class="caveat bad"><b>${r.outcome === "failed" ? "Exit 1 · " + esc((r.error || {}).kind || "") : "Interrupted"}</b>${esc((r.error || {}).message || "")}</div>`
      : "";
  box.innerHTML = `
    <div class="card">
      <div class="card-head"><h3>${esc(r.id)} · ${esc(r.dataset || "")}</h3><span class="hint">${pill(r)}</span></div>
      <div class="card-body">
        ${caveat}
        <div class="cols">
          <dl class="kv">${facts}</dl>
          <div class="log">${lines.map((l) => `<div class="ln"><span class="c">${fmtTime(l[0])}</span><span class="k ${l[3]}">${esc(l[1])}</span><span>${esc(l[2])}</span></div>`).join("")}</div>
        </div>
      </div>
    </div>`;
}

// ---- tables -----------------------------------------------------------------

const tablesState = { list: [], sel: null, snap: null, seq: 0 };

async function loadTables() {
  try {
    const data = await getJson("/api/tables");
    tablesState.list = data.tables || [];
    renderTablesList();
    if (!tablesState.sel && tablesState.list.length) openTable(tablesState.list[0].path);
    else if (tablesState.sel) openTable(tablesState.sel, tablesState.snap);
  } catch (e) {
    toast(e.message, "err");
  }
}

function renderTablesList() {
  const box = $("tables-list");
  if (!tablesState.list.length) {
    box.innerHTML = `<div class="empty" style="border:0">No Iceberg table under outputs/ yet.</div>`;
    return;
  }
  box.innerHTML = tablesState.list.map((t) => `
    <div class="tb${t.path === tablesState.sel ? " sel" : ""}" data-table="${esc(t.path)}">
      <span class="n">${esc(baseName(t.path))}</span>
      <span class="w">${esc(t.path)}</span>
      <span class="a">${t.rows == null ? "?" : fmtNum(t.rows)} rows<br>${t.snapshots} snapshots · ${fmtAgo(t.last_updated_ms)}</span>
    </div>`).join("");
}

async function openTable(path, snap) {
  if (!path) return;
  const seq = ++tablesState.seq;
  try {
    const t = await api("/api/table", { path });
    if (seq !== tablesState.seq) return; // a later open has the screen
    tablesState.sel = path;
    tablesState.snap = snap || null;
    renderTable(t);
    renderTablesList();
  } catch (e) {
    toast(e.message, "err");
  }
}

function eventWords(e) {
  const c = e.counts;
  let d;
  if (e.kind === "expire") {
    const seqs = e.removed.map((r) => r.sequence_number).filter((n) => n != null);
    d = `<b>${e.removed.length}</b> snapshot${e.removed.length === 1 ? "" : "s"} left the history${seqs.length ? ` (#${Math.min(...seqs)}–#${Math.max(...seqs)})` : ""}`;
  } else if (e.kind === "recreate") {
    d = `written afresh in place: the earlier table's <b>${e.removed.length}</b> snapshots replaced`;
  } else if (e.kind === "replace") {
    d = `compaction · <b>${fmtNum(c.removed_data_files)}</b> data files rewritten into <b>${fmtNum(c.added_data_files)}</b>${c.removed_delete_files ? ` · ${fmtNum(c.removed_delete_files)} delete files folded in` : ""} · rows unchanged`;
  } else {
    d = `<b>+${fmtNum(c.added_records)}</b> rows · <b>+${fmtNum(c.added_data_files)}</b> data file${c.added_data_files === 1 ? "" : "s"}`;
    if (c.added_position_deletes) d += ` · <b>${fmtNum(c.added_position_deletes)}</b> rows replaced (<b>+${fmtNum(c.added_delete_files)}</b> delete file${c.added_delete_files === 1 ? "" : "s"})`;
  }
  if (e.schema) {
    const add = e.schema.added.length ? `added <code>${e.schema.added.map(esc).join("</code>, <code>")}</code>` : "";
    const rm = e.schema.removed.length ? `removed <code>${e.schema.removed.map(esc).join("</code>, <code>")}</code>` : "";
    d += ` · schema <b>${e.schema.from} → ${e.schema.to}</b>${add || rm ? ": " + [add, rm].filter(Boolean).join("; ") : ""}`;
  }
  return d;
}

function eventWriter(e) {
  if (!e.snapshot_id) return "";
  const w = e.writer;
  if (w.run_id) {
    const known = runsState.runs.some((r) => r.id === w.run_id);
    const id = known ? `<a class="lnk" data-open-run="${esc(w.run_id)}">${esc(w.run_id)}</a>` : esc(w.run_id);
    return `${id}${w.rebases ? ` <span class="tag">${w.rebases} rebase${w.rebases === 1 ? "" : "s"}</span>` : ""}`;
  }
  return w.engine_version ? `shapeshift ${esc(w.engine_version)}` : `<span title="no shapeshift.version in the snapshot summary">another writer</span>`;
}

function filesChart(events) {
  const pts = events.filter((e) => e.snapshot_id && !e.expired).map((e) => [e.sequence_number, e.counts.total_data_files]);
  if (pts.length < 2) return "";
  const top = Math.max(...pts.map((p) => p[1])) * 1.1 || 1;
  const ymax = [10, 20, 30, 40, 50, 80, 100, 150, 200, 250, 300, 500, 1000].find((c) => c >= top) || Math.ceil(top / 1000) * 1000;
  const ticks = [0, ymax / 2, ymax];
  const n = pts.length, VX = 600, VY = 100;
  const px = (i) => (i / (n - 1)) * VX, py = (v) => VY - (v / ymax) * VY;
  const line = pts.map((p, i) => `${i ? "L" : "M"}${px(i).toFixed(1)} ${py(p[1]).toFixed(1)}`).join(" ");
  const hits = pts.map((p, i) => {
    const L = (Math.max(i - 0.5, 0) / (n - 1)) * 100, R = (Math.min(i + 0.5, n - 1) / (n - 1)) * 100;
    return `<span class="hit" style="left:${L.toFixed(2)}%;width:${(R - L).toFixed(2)}%" title="#${p[0]} · ${p[1]} data files"></span>`;
  }).join("");
  const last = pts[n - 1];
  return `<p class="hint" style="font-family:var(--font-mono);font-size:11px;color:var(--eg-faint);margin:0 0 8px">Data files after each snapshot</p>
    <div class="chart" role="img" aria-label="Data files after each snapshot">
      <div class="y">${ticks.map((v) => `<span style="bottom:${(v / ymax) * 100}%">${v}</span>`).join("")}</div>
      <div class="p">${ticks.slice(1).map((v) => `<span class="g" style="bottom:${(v / ymax) * 100}%"></span>`).join("")}
        <svg viewBox="0 0 ${VX} ${VY}" preserveAspectRatio="none" aria-hidden="true">
          <path class="a" d="${line} L${VX} ${VY} L0 ${VY} Z"/><path class="l" d="${line}" vector-effect="non-scaling-stroke"/></svg>
        <span class="dot" style="left:100%;top:${((py(last[1]) / VY) * 100).toFixed(2)}%"></span>${hits}</div>
      <div class="x"><span style="left:0">#${pts[0][0]}</span><span class="end" style="left:100%">#${last[0]}</span></div>
    </div>`;
}

function renderTable(t, target) {
  const events = (t.events || []).slice();
  const rows = events.slice().reverse().map((e) => `
    <div class="ev${e.expired ? " gone" : ""}${tablesState.snap && e.snapshot_id === tablesState.snap ? " hl" : ""}">
      <span class="s">${e.sequence_number != null && e.snapshot_id ? "#" + e.sequence_number : "—"}</span>
      <span class="k ek-${esc(e.kind)}">${esc(e.kind)}</span>
      <span class="d">${eventWords(e)}${e.expired ? ' <span class="tag">expired</span>' : ""}</span>
      <span class="r">${[fmtTime(e.timestamp_ms), eventWriter(e)].filter(Boolean).join(" · ")}</span>
    </div>`).join("");
  const debt = t.delete_files && t.data_files ? Math.round((t.delete_files / t.data_files) * 100) : 0;
  $(target || "table-detail").innerHTML = `
    <div class="card-head"><h3>${esc(baseName(t.path))}</h3><span class="hint">${esc(t.location || t.path)}</span></div>
    <div class="tstats">
      <div class="tstat"><div class="v">${t.rows == null ? "?" : fmtNum(t.rows)}</div><div class="k">rows</div></div>
      <div class="tstat"><div class="v">${fmtNum(t.data_files)}</div><div class="k">data files</div></div>
      <div class="tstat"><div class="v">${fmtNum(t.delete_files)}</div><div class="k">delete files${debt ? ` · ${debt}% of data files` : ""}</div></div>
      <div class="tstat"><div class="v">${t.snapshots}</div><div class="k">snapshots kept</div></div>
    </div>
    <div class="card-body">
      ${debt >= 25 ? `<div class="caveat"><b>Delete files are building up</b>Each merge leaves delete files that every read applies. <code>shapeshift compact ${esc(t.path)}</code> folds them back into the data.</div>` : ""}
      ${filesChart(events)}
      <div>${rows || '<div class="empty">This table has no snapshot yet.</div>'}</div>
      <dl class="kv" style="margin-top:6px">
        <dt>schema ${esc(String(t.schema_id))}</dt><dd>${(t.schema || []).map((f) => `${esc(f.name)} <span class="t2">${esc(f.type)}</span>`).join(" · ")}</dd>
        <dt>partitioned</dt><dd>${(t.partition || []).length ? t.partition.map(esc).join(", ") : "no"}</dd>
        <dt>read from</dt><dd>${t.versions_read == null ? "the catalog's current metadata" : `${t.versions_read} metadata version${t.versions_read === 1 ? "" : "s"}`}</dd>
      </dl>
    </div>`;
}

// ---- health -----------------------------------------------------------------

// The data dir, absolute, for the snippets that read a dataset's output.
let dataDir = "";

async function health() {
  try {
    const res = await fetch("/api/health").then((r) => r.json());
    dataDir = res.data_dir || "";
    const p = $("health-pill");
    p.className = "badge good";
    p.innerHTML = `<span class="dot"></span> v${res.version} · ${esc(baseName(res.data_dir) || res.data_dir)}`;
    p.title = "data dir: " + res.data_dir;
  } catch (_) {
    const p = $("health-pill");
    p.className = "badge danger";
    p.innerHTML = `<span class="dot"></span> offline`;
  }
}

// ---- overview -------------------------------------------------------------------

const ATTN = {
  run_failed: ["✕", "o-failed", "run failed"],
  run_conflict: ["75", "o-conflict", "lost a race"],
  table_stale: ["⏱", "o-interrupted", "stale"],
  rejects_over: ["!", "o-conflict", "rejects"],
};
const ovState = { timer: null };

async function loadOverview() {
  let data;
  try { data = await getJson("/api/overview"); } catch (_) { return; }
  getJson("/api/alerts").then((a) => firingBadge(a.incidents.filter((i) => i.state === "firing").length)).catch(() => {});
  const f = data.figures || {};
  const stat = (k, v, tone) => `<div class="stat"><div class="k">${k}</div><div class="v${tone ? " " + tone : ""}">${v}</div></div>`;
  $("ov-stats").innerHTML =
    stat("datasets", fmtNum(f.datasets) + `<span class="t2"> · ${fmtNum(f.watched)} watched</span>`)
    + stat("needs a look", fmtNum((data.attention || []).length), (data.attention || []).length ? "warn" : "good")
    + stat("runs · 24 h", fmtNum(f.runs_24h))
    + stat("failed · 24 h", fmtNum(f.failed_24h), f.failed_24h ? "warn" : "")
    + stat("running", fmtNum(f.running))
    + stat("tables", fmtNum(f.tables));
  const att = data.attention || [];
  const count = $("attention-count");
  count.classList.toggle("hidden", att.length === 0);
  count.textContent = att.length;
  $("ov-attention").innerHTML = att.length
    ? att.map((c) => {
      const a = ATTN[c.kind] || ["!", "o-failed", c.kind];
      return `<div class="tb attn" data-open-dataset="${esc(c.subject)}">
        <span class="n"><span class="pill ${a[1]}"><span class="g">${a[0]}</span>${esc(a[2])}</span> ${esc(c.subject)}</span>
        <span class="w">${esc(c.message)}</span>
        <span class="a">${c.severity === "critical" ? "critical" : "warning"}<br>${fmtAgo(c.since_ms)}</span>
      </div>`;
    }).join("")
    : `<div class="empty" style="border:0"><div class="et">Nothing needs a look</div>
        Every dataset's latest run committed, and the ones with a freshness window are inside it.
        ${f.datasets ? "" : `<br><a class="lnk" data-goto="datasets">Make a dataset</a> to have it watched here.`}</div>`;
  const recent = data.recent || [];
  $("ov-recent").innerHTML = recent.length
    ? recent.map((r) => `<div class="tb" data-open-run="${esc(r.id)}">
        <span class="n">${esc(r.dataset || "")} <span class="t2">${esc(r.id)}</span></span>
        <span class="w">${esc(r.output || "")}${r.via === "dataset" ? " · run now" : ""}</span>
        <span class="a">${pill(r)}<br>${fmtAgo(r.started_ms)}</span>
      </div>`).join("")
    : `<div class="empty" style="border:0">No runs yet.</div>`;
}

// ---- datasets -------------------------------------------------------------------

const dsState = { list: [], sel: null, editing: null, detail: null };
const FRESH = { fresh: ["badge good", "fresh"], stale: ["badge warn", "stale"], unwatched: ["badge", "not watched"] };

async function loadDatasets() {
  try {
    const data = await getJson("/api/datasets");
    dsState.list = data.datasets || [];
  } catch (_) { return; }
  renderDatasetList();
  if (dsState.editing) return;
  if (dsState.sel && dsState.list.some((d) => d.dataset.name === dsState.sel)) openDataset(dsState.sel);
}

function renderDatasetList() {
  const box = $("ds-list");
  if (!dsState.list.length) {
    box.innerHTML = `<div class="empty" style="border:0">No datasets yet. <a class="lnk" id="ds-first">Make one</a>.</div>`;
    return;
  }
  box.innerHTML = dsState.list.map((c) => {
    const d = c.dataset, r = c.latest_run, fr = FRESH[c.freshness] || FRESH.unwatched;
    return `<div class="tb${d.name === dsState.sel ? " sel" : ""}" data-ds="${esc(d.name)}">
      <span class="n">${esc(d.name)}</span>
      <span class="w">${esc(c.output)} · <span class="${fr[0]}" style="padding:0 5px">${fr[1]}</span></span>
      <span class="a">${c.running ? pill({ outcome: "running" }) : r ? pill(r) : "never run"}<br>${r ? fmtAgo(r.started_ms) : ""}</span>
    </div>`;
  }).join("");
}

async function openDataset(name) {
  dsState.sel = name;
  dsState.editing = null;
  let c;
  try { c = await api("/api/dataset", { name }); } catch (e) { toast(e.message, "err"); return; }
  dsState.detail = c;
  renderDatasetList();
  renderDataset(c);
}

function renderDataset(c) {
  const d = c.dataset, fr = FRESH[c.freshness] || FRESH.unwatched;
  const kv = (k, v) => `<dt>${k}</dt><dd>${v}</dd>`;
  const src = c.source;
  const pos = (p) => p ? `<span class="mono">${esc(JSON.stringify(Object.fromEntries(Object.entries(p).filter(([k]) => k !== "of"))))}</span>` : "none yet";
  const reads = src
    ? kv("reads", `<span class="mono">${esc(src.from)}</span> <span class="t2">${esc(src.kind)} source</span>`)
      + kv("position", pos(src.position))
      + kv("last pull", src.last ? `${fmtNum(src.last.new)} new · ${fmtNum(src.last.replayed)} re-read · ${fmtNum(src.last.already_read)} already read` : "none yet")
    : kv("reads", `<span class="mono">${esc(d.input_path || "—")}</span> <span class="t2">${esc(d.format)}</span>`);
  const facts = reads
    + kv("lands", `<span class="mono">${esc(c.output)}</span> <span class="t2">${esc(d.to)}${d.to === "iceberg" ? (d.append ? " · append" : " · replace") : ""} · ${esc(d.compression)}</span>`)
    + (d.partition_by && d.partition_by.length ? kv("partition", esc(d.partition_by.join(", "))) : "")
    + kv("schema", d.spec_yaml.trim() ? "from its spec" : "inferred on every run")
    + kv("drift", esc(d.on_drift || "the spec's (warn)"))
    + kv("freshness", d.freshness_minutes ? `within ${fmtNum(d.freshness_minutes)} min · <span class="${fr[0]}" style="padding:0 5px">${fr[1]}</span>` : "not watched")
    + kv("last success", c.last_ok_ms ? fmtAgo(c.last_ok_ms) : "none yet");
  const runs = c.runs || [];
  const runRows = runs.length
    ? runs.map((r) => `<div class="tb" data-open-run="${esc(r.id)}">
        <span class="n">${esc(r.id)}</span>
        <span class="w">${r.summary ? `${r.summary.source ? `${fmtNum(r.summary.source.new)} new · ` : ""}${fmtNum(r.summary.rows_out)} rows out · ${fmtNum((r.summary.rejected || 0) + (r.summary.parse_errors || 0))} rejected` : r.error ? esc(r.error.message).slice(0, 140) : ""}</span>
        <span class="a">${pill(r)}<br>${fmtAgo(r.started_ms)}</span>
      </div>`).join("")
    : `<div class="empty" style="border:0">Not run yet.</div>`;
  const abs = dataDir ? dataDir.replace(/\\/g, "/").replace(/\/$/, "") + "/" + c.output : c.output;
  const snippet = d.to === "iceberg"
    ? `-- DuckDB\nINSTALL iceberg; LOAD iceberg;\nSELECT * FROM iceberg_scan('${abs}') LIMIT 10;\n\n# PyIceberg (a table directory, no catalog)\nfrom pyiceberg.table import StaticTable\nt = StaticTable.from_metadata('${abs}/metadata/v<N>.metadata.json')`
    : `-- DuckDB\nSELECT * FROM read_parquet('${abs}') LIMIT 10;`;
  $("ds-detail").innerHTML = `
    <div class="card">
      <div class="card-head"><h3>${esc(d.name)}</h3>
        <span class="hint">
          <button class="btn sm primary" id="btn-ds-run"${c.running ? " disabled" : ""}>${c.running ? "Running…" : "Run now"}</button>
          <button class="btn sm ghost" id="btn-ds-edit">Edit</button>
          <button class="btn sm ghost" id="btn-ds-delete">Delete</button>
        </span></div>
      <div class="card-body">
        <dl class="kv">${facts}</dl>
        ${c.table ? `<div class="btn-row"><a class="lnk" data-open-table="${esc(c.output)}">Open its table history →</a></div>` : ""}
      </div>
    </div>
    <div class="card" style="margin-top:18px">
      <div class="card-head"><h3>Runs</h3><span class="hint">newest first</span></div>
      <div class="tb-list">${runRows}</div>
    </div>
    <div class="card" style="margin-top:18px">
      <div class="card-head"><h3>Rejected rows</h3><span class="hint">${esc(c.rejects_file)}</span></div>
      <div class="card-body" id="ds-rejects"><button class="btn sm ghost" id="btn-ds-rejects">Show the latest run's rejected rows</button></div>
    </div>
    <div class="card" style="margin-top:18px">
      <div class="card-head"><h3>Consume</h3><span class="hint">read what it landed</span></div>
      <div class="card-body"><pre class="codeline">${esc(snippet)}</pre></div>
    </div>`;
}

function datasetForm(d) {
  const isNew = !d;
  d = d || { name: "", input_path: "", format: "jsonl", to: "iceberg", compression: "snappy", append: true, partition_by: [], on_drift: null, freshness_minutes: null, spec_yaml: "" };
  const opt = (v, cur, label) => `<option value="${v}"${v === cur ? " selected" : ""}>${label || v}</option>`;
  dsState.editing = isNew ? "new" : d.name;
  $("ds-detail").innerHTML = `
    <div class="card">
      <div class="card-head"><h3>${isNew ? "New dataset" : "Edit " + esc(d.name)}</h3></div>
      <div class="card-body stack">
        <label class="field"><span class="lh">Name <span class="note">letters, digits, _ and -; it is also the output's name</span></span>
          <input class="ctrl mono-input" id="dsf-name" value="${esc(d.name)}"${isNew ? "" : " disabled"} /></label>
        <label class="field"><span class="lh">Input file <span class="note">on this machine: relative to the data dir, or absolute; empty when the spec's <span class="mono">source</span> pulls (an endpoint or a database)</span></span>
          <input class="ctrl mono-input" id="dsf-input" value="${esc(d.input_path)}" placeholder="/data/events.jsonl" /></label>
        <div class="row">
          <label class="field"><span class="lh">Source format</span><select class="ctrl" id="dsf-format">${opt("jsonl", d.format)}${opt("json-array", d.format)}</select></label>
          <label class="field"><span class="lh">Output</span><select class="ctrl" id="dsf-to">${opt("iceberg", d.to, "Iceberg v2 table")}${opt("parquet", d.to, "Parquet file")}</select></label>
          <label class="field"><span class="lh">Compression</span><select class="ctrl" id="dsf-compression">${opt("snappy", d.compression)}${opt("uncompressed", d.compression)}${opt("zstd", d.compression)}</select></label>
        </div>
        <div class="row">
          <label class="field"><span class="lh">Partition by <span class="note">Iceberg; comma-separated</span></span>
            <input class="ctrl mono-input" id="dsf-partition" value="${esc((d.partition_by || []).join(", "))}" placeholder="day(event_at), region" /></label>
          <label class="field"><span class="lh">On drift</span><select class="ctrl" id="dsf-drift">${opt("", d.on_drift || "", "the spec's")}${["warn", "rescue", "quarantine", "error", "ignore"].map((p) => opt(p, d.on_drift || "")).join("")}</select></label>
          <label class="field"><span class="lh">Freshness <span class="note">minutes; empty = not watched</span></span>
            <input class="ctrl mono-input" id="dsf-fresh" type="number" min="1" value="${d.freshness_minutes || ""}" /></label>
        </div>
        <label class="switch"><input type="checkbox" id="dsf-append"${d.append ? " checked" : ""} /><span class="track"></span> Iceberg: append a snapshot each run (off: replace the table)</label>
        <label class="field"><span class="lh">Spec <span class="note">YAML; empty = infer the schema on every run. Its merge_on and where apply.</span></span>
          <textarea class="ctrl mono-input" id="dsf-spec" rows="12">${esc(d.spec_yaml)}</textarea></label>
        <div class="btn-row">
          <button class="btn primary" id="btn-dsf-save">Save</button>
          <button class="btn ghost" id="btn-dsf-cancel">Cancel</button>
        </div>
      </div>
    </div>`;
}

async function saveDataset() {
  const fresh = $("dsf-fresh").value.trim();
  const body = {
    name: $("dsf-name").value.trim(),
    input_path: $("dsf-input").value.trim(),
    format: $("dsf-format").value,
    to: $("dsf-to").value,
    compression: $("dsf-compression").value,
    append: $("dsf-append").checked,
    partition_by: $("dsf-partition").value.trim() ? [$("dsf-partition").value.trim()] : [],
    on_drift: $("dsf-drift").value || null,
    freshness_minutes: fresh ? Number(fresh) : null,
    spec_yaml: $("dsf-spec").value,
  };
  try {
    await api("/api/dataset/save", body);
  } catch (e) { toast(e.message, "err"); return; }
  toast(`Saved ${body.name}.`, "ok");
  dsState.editing = null;
  dsState.sel = body.name;
  await loadDatasets();
}

async function runDataset() {
  const name = dsState.sel;
  if (!name) return;
  const btn = $("btn-ds-run");
  if (btn) { btn.disabled = true; btn.textContent = "Running…"; }
  try {
    const res = await api("/api/dataset/run", { name });
    toast(`${name}: ${fmtNum(res.rows_out)} rows landed${res.rejects_total ? `, ${fmtNum(res.rejects_total)} rejected` : ""}.`, "ok");
  } catch (e) { toast(`${name}: ${e.message}`, "err"); }
  await loadDatasets();
  loadOverview();
}

async function deleteDataset() {
  const name = dsState.sel;
  if (!name || !confirm(`Forget the dataset ${name}? Its output and its runs stay.`)) return;
  try { await api("/api/dataset/delete", { name }); } catch (e) { toast(e.message, "err"); return; }
  toast(`Forgot ${name}.`, "ok");
  dsState.sel = null;
  $("ds-detail").innerHTML = "";
  await loadDatasets();
}

async function showRejects() {
  let res;
  try { res = await api("/api/dataset/rejects", { name: dsState.sel }); } catch (e) { toast(e.message, "err"); return; }
  const box = $("ds-rejects");
  if (!res.total) { box.innerHTML = `<div class="t2">The latest run rejected nothing.</div>`; return; }
  box.innerHTML = `<div class="t2" style="margin-bottom:8px">${fmtNum(res.total)} rejected${res.total > res.rows.length ? `, the first ${fmtNum(res.rows.length)} shown` : ""}.</div>
    <div class="rejects">${res.rows.map((r) => `<div class="reject"><div class="rh">${r.line ? `<span class="rl">line ${r.line}</span>` : ""}<span class="re">${esc(r.error || "")}</span></div><div class="rr">${esc(typeof r.raw === "string" ? r.raw : JSON.stringify(r.raw))}</div></div>`).join("")}</div>`;
}

// ---- catalog --------------------------------------------------------------------

const ctState = { data: null, sel: null, seq: 0 };

async function loadCatalog(fresh) {
  $("ct-list").innerHTML = '<div class="empty" style="border:0">Reading the catalog…</div>';
  try {
    ctState.data = fresh ? await api("/api/catalog/refresh", {}) : await getJson("/api/catalog");
  } catch (e) {
    $("ct-list").innerHTML = `<div class="empty" style="border:0"><div class="et">Could not read the catalog</div>${esc(e.message)}</div>`;
    return;
  }
  renderCatalog();
}

function renderCatalog() {
  const d = ctState.data;
  const head = $("ct-head");
  if (!d.available) {
    head.innerHTML = `<b>Not in this build</b>Browsing a REST catalog needs a build with <span class="mono">object_store</span>: <span class="mono">cargo build -p shapeshift-cli --features serve,object_store</span>.`;
  } else if (!d.configured) {
    head.innerHTML = `<b>No catalog</b>Start the console with <span class="mono">--catalog &lt;uri&gt;</span> (and <span class="mono">--warehouse</span>) to browse one, read-only. Credentials come from <span class="mono">SHAPESHIFT_CATALOG_TOKEN</span> or <span class="mono">SHAPESHIFT_CATALOG_CREDENTIAL</span>, as for <span class="mono">shape --catalog</span>.`;
  } else {
    head.innerHTML = `<b>Read-only</b>${esc(d.uri)}${d.warehouse ? " · warehouse " + esc(d.warehouse) : ""}${d.insecure ? " · insecure: credentials may go over plain http" : ""}. Nothing here creates, commits or registers.`;
  }
  const list = $("ct-list");
  if (!d.configured) { list.innerHTML = ""; return; }
  if (d.error) { list.innerHTML = `<div class="empty" style="border:0"><div class="et">${d.error_kind === "config" ? "This console's catalog settings are wrong" : "The catalog did not answer"}</div>${esc(d.error)}</div>`; return; }
  const ns = d.namespaces || [];
  list.innerHTML = ns.length ? ns.map((n) => `
    <div class="tb" style="cursor:default"><span class="n">${esc(n.namespace)}</span>
      <span class="w">${n.error ? `<span style="color:var(--eg-danger)">${esc(n.error)}</span>` : pl(n.tables.length, "table", "tables")}</span><span class="a"></span></div>
    ${n.tables.map((t) => `<div class="tb${ctState.sel === n.namespace + "." + t ? " sel" : ""}" data-ct-table="${esc(t)}" data-ct-levels="${esc(JSON.stringify(n.levels))}" style="padding-left:28px">
      <span class="n">${esc(t)}</span><span class="w">${esc(n.namespace)}.${esc(t)}</span><span class="a"></span></div>`).join("")}`).join("")
    : `<div class="empty" style="border:0">The catalog has no namespaces.</div>`;
  if (d.truncated) list.innerHTML += `<div class="caveat" style="margin:10px">Showing the first ${fmtNum(ns.length)} namespaces: the catalog holds more.</div>`;
  if (d.listed_ms) list.innerHTML += `<div class="t2" style="padding:8px 14px">listed ${fmtAgo(d.listed_ms)} · Refresh lists again</div>`;
}

async function openCatalogTable(levels, name) {
  ctState.sel = levels.join(".") + "." + name;
  renderCatalog();
  const seq = ++ctState.seq;
  let t;
  try { t = await api("/api/catalog/table", { levels, name }); } catch (e) { if (seq === ctState.seq) toast(e.message, "err"); return; }
  if (seq !== ctState.seq) return; // a later click has the screen
  tablesState.snap = null;
  renderTable(t, "ct-detail");
  renderEngines(ctState.data, levels, name);
}

function renderEngines(d, levels, name) {
  const ident = levels.join(".") + "." + name;
  const wh = d.warehouse || "<warehouse>";
  const snippets = [
    ["DuckDB", `INSTALL iceberg; LOAD iceberg;
CREATE SECRET cat_secret (TYPE iceberg, TOKEN '<token>');   -- or CLIENT_ID / CLIENT_SECRET
ATTACH '${wh}' AS cat (TYPE iceberg, SECRET cat_secret, ENDPOINT '${d.uri}');
SELECT * FROM cat.${ident} LIMIT 10;`],
    ["Spark", `--conf spark.sql.catalog.cat=org.apache.iceberg.spark.SparkCatalog
--conf spark.sql.catalog.cat.type=rest
--conf spark.sql.catalog.cat.uri=${d.uri}
--conf spark.sql.catalog.cat.warehouse=${wh}
--conf spark.sql.catalog.cat.token=<token>
-- then: SELECT * FROM cat.${ident} LIMIT 10`],
    ["PyIceberg", `from pyiceberg.catalog import load_catalog
cat = load_catalog("cat", type="rest", uri="${d.uri}", warehouse="${wh}", token="<token>")
print(cat.load_table("${ident}").scan(limit=10).to_arrow())`],
  ];
  const box = $("ct-engines");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="card-head"><h3>Read it from an engine</h3><span class="hint">credentials left as placeholders</span></div>
    <div class="card-body stack">${snippets.map(([k, v]) => `<div><div class="t2" style="margin-bottom:4px">${k}</div><pre class="codeline">${esc(v)}</pre></div>`).join("")}</div>`;
}

// ---- connectors -----------------------------------------------------------------

const cnState = { data: null };

async function loadConnectors() {
  try { cnState.data = await getJson("/api/connectors"); } catch (_) { return; }
  renderConnectors();
}

function ingestBase() {
  const a = cnState.data && cnState.data.ingest_addr;
  if (!a) return null;
  // A listener on every interface is reached by this machine's name; show the port.
  return "http://" + a.replace(/^0\.0\.0\.0:/, location.hostname + ":").replace(/^\[::\]:/, location.hostname + ":");
}

function renderConnectors() {
  const d = cnState.data;
  const base = ingestBase();
  $("cn-listener").innerHTML = base
    ? `<b>Ingest listener on</b>Producers POST to <span class="mono">${esc(base)}/v1/ingest/&lt;connector&gt;</span> with the connector's token. That listener serves that path only; this console stays where it is.`
    : `<b>Ingest listener off</b>Connectors keep their settings but take nothing until the console starts with <span class="mono">--ingest-addr</span> (for example <span class="mono">shapeshift serve --ingest-addr 0.0.0.0:8088</span>).`;
  $("cn-list").innerHTML = d.connectors.length
    ? d.connectors.map((c) => {
      const s = c.stats;
      const waiting = c.spool_bytes + c.waiting_bytes;
      return `<div class="tb">
        <span class="n">${esc(c.name)} <span class="t2">→ <a class="lnk" data-open-dataset="${esc(c.dataset)}">${esc(c.dataset)}</a> · token …${esc(c.token_hint)}</span></span>
        <span class="w">${pl(s.records, "record", "records")} in ${pl(s.posts, "post", "posts")} · ${pl(s.runs_ok, "batch", "batches")} landed${s.runs_failed ? ` · <span style="color:var(--eg-danger)">${fmtNum(s.runs_failed)} failed</span>` : ""} · ${pl(s.duplicates, "retry", "retries")} ignored · ${fmtNum(s.refused)} refused
          <br>${waiting ? `${fmtBytes(waiting)} waiting${c.waiting_batches ? ` (${c.waiting_batches} sealed)` : ""}` : "nothing waiting"} · seals at ${fmtBytes(c.flush_bytes)} or ${fmtNum(c.flush_seconds)} s${s.last_post_ms ? " · last post " + fmtAgo(s.last_post_ms) : ""}
          ${s.last_error ? `<br><span style="color:var(--eg-danger)">${esc(s.last_error)}</span>` : ""}</span>
        <span class="a"><button class="btn sm ghost" data-cn-flush="${esc(c.name)}">Flush now</button> <button class="btn sm ghost" data-cn-rotate="${esc(c.name)}">New token</button> <button class="btn sm ghost" data-cn-edit="${esc(c.name)}">Edit</button></span>
      </div>`;
    }).join("")
    : `<div class="empty" style="border:0"><div class="et">No connectors</div>A connector is an ingest URL a producer POSTs JSON records to, with its own token. Its records are batched and landed by an Iceberg dataset.</div>`;
}

function showToken(name, token) {
  const base = ingestBase() || "http://<ingest-addr>";
  const box = $("cn-token");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="card"><div class="card-head"><h3>Token for ${esc(name)}</h3><span class="hint">shown once: copy it now</span></div>
    <div class="card-body"><pre class="codeline">${esc(token)}</pre>
    <div class="t2" style="margin:10px 0 6px">Send records:</div>
    <pre class="codeline">${esc(`curl -X POST ${base}/v1/ingest/${name} \\\n  -H "Authorization: Bearer ${token}" \\\n  -H "Content-Type: application/x-ndjson" \\\n  --data-binary @records.jsonl`)}</pre>
    ${["http://localhost", "http://127.", "http://[::1]"].some((p) => base.startsWith(p)) ? "" : `<div class="caveat bad" style="margin-top:10px"><b>Plain HTTP</b>This URL is not on this machine: the token and the records would cross the network unencrypted. Put a TLS-terminating proxy in front of the ingest listener and give producers its https:// URL, or keep them on a network you trust.</div>`}
    <div class="btn-row"><button class="btn sm ghost" data-form-close="cn-token">I have copied it</button></div></div></div>`;
}

function pl(n, one, many) { return `${fmtNum(n)} ${n === 1 ? one : many}`; }

function connectorForm(c) {
  const isNew = !c;
  c = c || { name: "", dataset: "", flush_bytes: 1048576, flush_seconds: 60 };
  const ice = dsState.list.filter((x) => x.dataset.to === "iceberg");
  const box = $("cn-form");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="stack">
    <div class="row">
      <label class="field"><span class="lh">Name <span class="note">it is in the URL</span></span><input class="ctrl mono-input" id="cf-name" value="${esc(c.name)}"${isNew ? "" : " disabled"} /></label>
      <label class="field"><span class="lh">Dataset <span class="note">an Iceberg dataset lands each batch</span></span><select class="ctrl" id="cf-dataset">${ice.length ? (ice.some((x) => x.dataset.name === c.dataset) ? "" : `<option value="" selected>${c.dataset ? `choose a dataset (${esc(c.dataset)} is gone)` : "choose a dataset"}</option>`) + ice.map((x) => `<option${x.dataset.name === c.dataset ? " selected" : ""}>${esc(x.dataset.name)}</option>`).join("") : '<option value="">no Iceberg dataset yet</option>'}</select></label>
    </div>
    <div class="row">
      <label class="field"><span class="lh">Seal at bytes</span><input class="ctrl mono-input" id="cf-bytes" type="number" min="1024" value="${c.flush_bytes}" /></label>
      <label class="field"><span class="lh">or after seconds</span><input class="ctrl mono-input" id="cf-secs" type="number" min="1" value="${c.flush_seconds}" /></label>
    </div>
    <div class="btn-row"><button class="btn primary" data-cn-save="${isNew ? "" : esc(c.name)}">${isNew ? "Make it" : "Save"}</button>
      ${isNew ? "" : `<button class="btn ghost" data-cn-delete="${esc(c.name)}">Delete</button>`}
      <button class="btn ghost" data-form-close="cn-form">Cancel</button></div>
  </div>`;
}

async function connectorsAction(ev) {
  const el = ev.target.closest("[data-cn-flush],[data-cn-rotate],[data-cn-edit],[data-cn-save],[data-cn-delete],[data-form-close]");
  if (!el) return;
  const ds = el.dataset;
  const call = async (path, body, okMsg) => {
    try { const r = await api(path, body); if (okMsg) toast(okMsg, "ok"); return r; } catch (e) { toast(e.message, "err"); return null; }
  };
  if (ds.formClose) { $(ds.formClose).classList.add("hidden"); return; }
  if (ds.cnFlush !== undefined) {
    const r = await call("/api/connector/flush", { name: ds.cnFlush });
    if (r) toast(r.sealed ? "Sealed: its batch lands in a moment." : "Nothing in the spool; a waiting batch is tried now.", "ok");
  } else if (ds.cnRotate !== undefined) {
    if (!confirm("Make a new token? The old one stops working at once.")) return;
    const r = await call("/api/connector/rotate", { name: ds.cnRotate });
    if (r) showToken(ds.cnRotate, r.token);
  } else if (ds.cnEdit !== undefined) {
    await loadDatasetsQuiet();
    connectorForm(cnState.data.connectors.find((c) => c.name === ds.cnEdit));
    return;
  } else if (ds.cnSave !== undefined) {
    const body = { name: ds.cnSave || $("cf-name").value.trim(), dataset: $("cf-dataset").value, flush_bytes: Number($("cf-bytes").value), flush_seconds: Number($("cf-secs").value) };
    const r = await call("/api/connector/save", body, ds.cnSave ? "Saved." : null);
    if (!r) return;
    $("cn-form").classList.add("hidden");
    if (r.token) showToken(body.name, r.token);
  } else if (ds.cnDelete !== undefined) {
    let r = await api("/api/connector/delete", { name: ds.cnDelete }).catch((e) => ({ error: e.message }));
    if (r.error && /waiting/.test(r.error)) {
      if (!confirm(r.error + "\n\nDelete anyway and discard them?")) return;
      r = await call("/api/connector/delete", { name: ds.cnDelete, discard: true }, "Deleted; its waiting records were discarded.");
    } else if (r.error) { toast(r.error, "err"); return; } else toast("Deleted.", "ok");
    $("cn-form").classList.add("hidden");
  }
  await loadConnectors();
}

// ---- alerts ---------------------------------------------------------------------

const KIND_WORDS = {
  run_failed: "a dataset's latest run failed",
  run_conflict: "it lost a race to another writer (exit 75)",
  table_stale: "no successful run within its freshness window",
  rejects_over: "its latest run rejected more rows than a threshold",
};
const alState = { data: null };

async function loadAlerts() {
  try { alState.data = await getJson("/api/alerts"); } catch (_) { return; }
  renderAlerts();
}

function firingBadge(n) {
  const c = $("firing-count");
  c.classList.toggle("hidden", !n);
  c.textContent = n;
}

function renderAlerts() {
  const d = alState.data;
  const firing = d.incidents.filter((i) => i.state === "firing");
  firingBadge(firing.length);
  const tname = (id) => (d.targets.find((t) => t.id === id) || { name: id }).name;
  $("al-incidents").innerHTML = d.incidents.length
    ? d.incidents.slice(0, 60).map((i) => {
      const a = ATTN[i.kind] || ATTN.rejects_over;
      return `<div class="tb${i.state === "firing" ? "" : " done"}">
        <span class="n"><span class="pill ${i.state === "firing" ? a[1] : "o-ok"}"><span class="g">${i.state === "firing" ? a[0] : "✓"}</span>${i.state}</span>
          <a class="lnk" data-open-dataset="${esc(i.subject)}">${esc(i.subject)}</a> <span class="t2">· ${esc(i.rule_name)}</span></span>
        <span class="w">${esc(i.message)}</span>
        <span class="a">${i.state === "firing" ? `<button class="btn sm ghost" data-resolve="${esc(i.id)}">Resolve</button><br>opened ${fmtAgo(i.opened_ms)}`
          : `resolved ${fmtAgo(i.resolved_ms)}<br>${esc(i.resolved_by === "hand" ? "by hand" : i.resolved_by === "rule" ? "rule changed" : "condition cleared")}`}</span>
      </div>`;
    }).join("")
    : `<div class="empty" style="border:0"><div class="et">No incidents</div>When a rule's condition starts to hold for a dataset, an incident opens here and its targets are told.</div>`;
  $("al-rules").innerHTML = d.rules.length
    ? d.rules.map((r) => `<div class="tb" data-rule="${esc(r.id)}">
        <span class="n">${esc(r.name)} ${r.enabled ? "" : '<span class="tag">off</span>'}</span>
        <span class="w">${esc(r.kind)}${r.kind === "rejects_over" ? " " + fmtNum(r.threshold || 0) : ""} · ${esc(r.dataset || "every dataset")} → ${r.targets.length ? r.targets.map((t) => esc(tname(t))).join(", ") : "no target"}</span>
        <span class="a"><button class="btn sm ghost" data-edit-rule="${esc(r.id)}">Edit</button></span>
      </div>`).join("")
    : `<div class="empty" style="border:0">No rules yet.${d.targets.length ? "" : " Add a target first, then a rule that tells it."}</div>`;
  $("al-targets").innerHTML = d.targets.length
    ? d.targets.map((t) => {
      const l = t.last;
      const last = l ? `<span class="badge ${l.ok ? "good" : "danger"}" style="padding:0 5px">${l.ok ? "delivered" : "failed"}</span> ${esc(l.event)} · ${fmtAgo(l.at_ms)}${l.ok ? "" : ` · ${esc(l.error || "")}`}` : "nothing sent yet";
      return `<div class="tb">
        <span class="n">${esc(t.name)} <span class="tag">${esc(t.kind)}</span></span>
        <span class="w"><span class="mono">${esc(t.url)}</span><br>${last}</span>
        <span class="a"><button class="btn sm ghost" data-test-target="${esc(t.id)}">Test</button> <button class="btn sm ghost" data-edit-target="${esc(t.id)}">Edit</button></span>
      </div>`;
    }).join("")
    : `<div class="empty" style="border:0">No targets yet.</div>`;
}

function ruleForm(r) {
  const d = alState.data;
  const isNew = !r;
  r = r || { name: "", kind: "run_failed", dataset: null, threshold: null, targets: [], enabled: true };
  const opt = (v, cur, label) => `<option value="${esc(v)}"${v === cur ? " selected" : ""}>${esc(label || v)}</option>`;
  const box = $("al-rule-form");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="stack">
    <label class="field"><span class="lh">Name</span><input class="ctrl" id="rf-name" value="${esc(r.name)}" /></label>
    <label class="field"><span class="lh">When</span><select class="ctrl" id="rf-kind">${d.kinds.map((k) => opt(k, r.kind, k + " — " + KIND_WORDS[k])).join("")}</select></label>
    <div class="row">
      <label class="field"><span class="lh">Dataset</span><select class="ctrl" id="rf-dataset">${opt("", r.dataset || "", "every dataset")}${dsState.list.map((c) => opt(c.dataset.name, r.dataset || "")).join("")}</select></label>
      <label class="field"><span class="lh">More than <span class="note">rejected rows; rejects_over only</span></span><input class="ctrl mono-input" id="rf-threshold" type="number" min="0" value="${r.threshold == null ? "" : r.threshold}" /></label>
    </div>
    <div class="field"><span class="lh">Tell</span>${d.targets.length ? d.targets.map((t) => `<label class="switch"><input type="checkbox" class="rf-target" value="${esc(t.id)}"${r.targets.includes(t.id) ? " checked" : ""} /><span class="track"></span> ${esc(t.name)}</label>`).join("") : '<span class="t2">no targets yet</span>'}</div>
    <label class="switch"><input type="checkbox" id="rf-enabled"${r.enabled ? " checked" : ""} /><span class="track"></span> On</label>
    <div class="btn-row"><button class="btn primary" data-rule-save="${isNew ? "" : esc(r.id)}">Save</button>
      ${isNew ? "" : `<button class="btn ghost" data-rule-delete="${esc(r.id)}">Delete</button>`}
      <button class="btn ghost" data-form-close="al-rule-form">Cancel</button></div>
  </div>`;
}

function targetForm(t) {
  const isNew = !t;
  t = t || { name: "", kind: "webhook" };
  const box = $("al-target-form");
  box.classList.remove("hidden");
  box.innerHTML = `<div class="stack">
    <div class="row">
      <label class="field"><span class="lh">Name</span><input class="ctrl" id="tf-name" value="${esc(t.name)}" /></label>
      <label class="field"><span class="lh">Kind</span><select class="ctrl" id="tf-kind"><option value="webhook"${t.kind === "webhook" ? " selected" : ""}>webhook</option><option value="slack"${t.kind === "slack" ? " selected" : ""}>slack</option></select></label>
    </div>
    <label class="field"><span class="lh">URL ${isNew ? "" : '<span class="note">leave empty to keep the saved one</span>'}</span><input class="ctrl mono-input" id="tf-url" type="password" autocomplete="off" placeholder="https://hooks.slack.com/services/…" /></label>
    <div class="btn-row"><button class="btn primary" data-target-save="${isNew ? "" : esc(t.id)}">Save</button>
      ${isNew ? "" : `<button class="btn ghost" data-target-delete="${esc(t.id)}">Delete</button>`}
      <button class="btn ghost" data-form-close="al-target-form">Cancel</button></div>
  </div>`;
}

async function alertsAction(ev) {
  const el = ev.target.closest("[data-resolve],[data-edit-rule],[data-edit-target],[data-test-target],[data-rule-save],[data-rule-delete],[data-target-save],[data-target-delete],[data-form-close]");
  if (!el) return;
  const ds = el.dataset;
  const call = async (path, body, okMsg) => {
    try { const r = await api(path, body); if (okMsg) toast(okMsg, "ok"); return r; } catch (e) { toast(e.message, "err"); return null; }
  };
  if (ds.formClose) { $(ds.formClose).classList.add("hidden"); return; }
  if (ds.resolve !== undefined) await call("/api/alerts/incident/resolve", { id: ds.resolve }, "Resolved. It stays quiet until its condition clears.");
  else if (ds.editRule !== undefined) { if (!dsState.list.length) await loadDatasetsQuiet(); ruleForm(alState.data.rules.find((r) => r.id === ds.editRule)); return; }
  else if (ds.editTarget !== undefined) { targetForm(alState.data.targets.find((t) => t.id === ds.editTarget)); return; }
  else if (ds.testTarget !== undefined) {
    const r = await call("/api/alerts/target/test", { id: ds.testTarget });
    if (r) toast(r.delivery.ok ? `Delivered (${r.delivery.status}).` : `Not delivered: ${r.delivery.error || r.delivery.status}`, r.delivery.ok ? "ok" : "err");
  } else if (ds.ruleSave !== undefined) {
    const th = $("rf-threshold").value.trim();
    const kind = $("rf-kind").value;
    const body = {
      name: $("rf-name").value, kind, dataset: $("rf-dataset").value || null,
      threshold: kind === "rejects_over" && th !== "" ? Number(th) : null,
      targets: [...document.querySelectorAll(".rf-target:checked")].map((x) => x.value),
      enabled: $("rf-enabled").checked,
    };
    if (ds.ruleSave) body.id = ds.ruleSave;
    if (!(await call("/api/alerts/rule/save", body, "Rule saved."))) return;
    $("al-rule-form").classList.add("hidden");
  } else if (ds.ruleDelete !== undefined) {
    if (!confirm("Delete this rule? Its firing incidents resolve without a notice.")) return;
    if (!(await call("/api/alerts/rule/delete", { id: ds.ruleDelete }, "Rule deleted."))) return;
    $("al-rule-form").classList.add("hidden");
  } else if (ds.targetSave !== undefined) {
    const body = { name: $("tf-name").value, kind: $("tf-kind").value, url: $("tf-url").value || null };
    if (ds.targetSave) body.id = ds.targetSave;
    if (!(await call("/api/alerts/target/save", body, "Target saved."))) return;
    $("al-target-form").classList.add("hidden");
  } else if (ds.targetDelete !== undefined) {
    if (!confirm("Delete this target?")) return;
    if (!(await call("/api/alerts/target/delete", { id: ds.targetDelete }, "Target deleted."))) return;
    $("al-target-form").classList.add("hidden");
  }
  await loadAlerts();
}

async function loadDatasetsQuiet() {
  try { dsState.list = (await getJson("/api/datasets")).datasets || []; } catch (_) { /* the form offers "every dataset" */ }
}

// ---- the spec follows the controls ---------------------------------------------
// The server applies the console's controls over the spec (dataset, source format, output
// format and compression; the path is managed under outputs/). Mirror them into the editor so
// it never shows a spec the run will not use. Only the lines this console's own YAML writes are
// touched; a spec laid out another way is left as typed.
function syncSpecToControls() {
  const ta = $("spec-yaml");
  if (!ta.value.trim()) return;
  const dataset = ($("out-dataset").value || "").trim() || "events";
  const to = $("out-format").value;
  const want = {
    "": { dataset },
    source: { format: $("src-format").value },
    output: { format: to, path: to === "iceberg" ? dataset : dataset + ".parquet", compression: $("out-compression").value },
  };
  let block = "";
  const lines = ta.value.split("\n").map((line) => {
    const top = /^([A-Za-z_][\w-]*):(.*)$/.exec(line);
    if (top) {
      block = top[1];
      return top[1] === "dataset" ? "dataset: " + want[""].dataset : line;
    }
    const child = /^  ([A-Za-z_][\w-]*):( .*)?$/.exec(line);
    const set = want[block];
    if (child && set && Object.prototype.hasOwnProperty.call(set, child[1])) return "  " + child[1] + ": " + set[child[1]];
    return line;
  });
  const next = lines.join("\n");
  if (next !== ta.value) ta.value = next;
}

// ---- wire up ----------------------------------------------------------------

function toggleIcebergOpts() {
  $("iceberg-opts").classList.toggle("hidden", $("out-format").value !== "iceberg");
}

function init() {
  document.querySelectorAll(".nav-item").forEach((n) => {
    // Reachable and usable from the keyboard as well as the mouse.
    n.setAttribute("role", "button");
    n.tabIndex = 0;
    n.addEventListener("click", () => showView(n.dataset.view));
    n.addEventListener("keydown", (e) => {
      if (e.key === "Enter" || e.key === " ") { e.preventDefault(); showView(n.dataset.view); }
    });
  });
  $("btn-sample").addEventListener("click", () => { $("src-input").value = SAMPLE; toast("Sample loaded — 4 good records + 1 bad line.", "ok"); });
  $("btn-clear-input").addEventListener("click", () => { $("src-input").value = ""; });
  $("btn-infer").addEventListener("click", doInfer);
  $("btn-shape").addEventListener("click", doShape);
  $("btn-inspect").addEventListener("click", doInspect);
  $("out-format").addEventListener("change", toggleIcebergOpts);
  for (const id of ["out-dataset", "out-format", "out-compression", "src-format"]) {
    $(id).addEventListener(id === "out-dataset" ? "input" : "change", syncSpecToControls);
  }
  $("runs-list").addEventListener("click", (ev) => {
    const row = ev.target.closest("[data-run]");
    if (row) { runsState.sel = row.dataset.run; renderRuns(); }
  });
  $("tables-list").addEventListener("click", (ev) => {
    const row = ev.target.closest("[data-table]");
    if (row) openTable(row.dataset.table);
  });
  $("btn-table-open").addEventListener("click", () => openTable($("table-path").value.trim()));
  // Runs link to the snapshot they committed, and snapshots to the run that wrote them.
  document.addEventListener("click", (ev) => {
    const t = ev.target.closest("[data-open-table]");
    if (t) { tablesState.sel = t.dataset.openTable; tablesState.snap = t.dataset.snap; showView("tables"); return; }
    const r = ev.target.closest("[data-open-run]");
    if (r) { runsState.sel = r.dataset.openRun; showView("runs"); }
  });
  // Catalog.
  $("btn-ct-refresh").addEventListener("click", () => loadCatalog(true));
  $("ct-list").addEventListener("click", (ev) => {
    const row = ev.target.closest("[data-ct-table]");
    if (row) openCatalogTable(JSON.parse(row.dataset.ctLevels), row.dataset.ctTable);
  });
  // Connectors and alerts.
  $("view-connectors").addEventListener("click", connectorsAction);
  $("btn-cn-new").addEventListener("click", async () => { await loadDatasetsQuiet(); connectorForm(null); });
  for (const id of ["view-alerts"]) $(id).addEventListener("click", alertsAction);
  $("btn-rule-new").addEventListener("click", async () => { if (!dsState.list.length) await loadDatasetsQuiet(); ruleForm(null); });
  $("btn-target-new").addEventListener("click", () => targetForm(null));
  // The firing count follows the overview's refresh.
  // Overview and datasets.
  $("btn-ds-new").addEventListener("click", () => { dsState.sel = null; renderDatasetList(); datasetForm(null); });
  $("ds-list").addEventListener("click", (ev) => {
    if (ev.target.closest("#ds-first")) { datasetForm(null); return; }
    const row = ev.target.closest("[data-ds]");
    if (row) openDataset(row.dataset.ds);
  });
  $("ds-detail").addEventListener("click", (ev) => {
    const id = (ev.target.closest("button") || {}).id;
    if (id === "btn-ds-run") runDataset();
    else if (id === "btn-ds-edit") { const c = dsState.list.find((x) => x.dataset.name === dsState.sel); if (c) datasetForm(c.dataset); }
    else if (id === "btn-ds-delete") deleteDataset();
    else if (id === "btn-ds-rejects") showRejects();
    else if (id === "btn-dsf-save") saveDataset();
    else if (id === "btn-dsf-cancel") { dsState.editing = null; if (dsState.sel) openDataset(dsState.sel); else $("ds-detail").innerHTML = ""; }
  });
  document.addEventListener("click", (ev) => {
    const g = ev.target.closest("[data-goto]");
    if (g) { showView(g.dataset.goto); return; }
    const d = ev.target.closest("[data-open-dataset]");
    if (d) { showView("datasets"); openDataset(d.dataset.openDataset); }
  });
  toggleIcebergOpts();
  health();
  loadRuns();
  showView("overview");
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
