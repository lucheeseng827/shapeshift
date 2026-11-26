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
  shape: ["Shape", "Infer a spec, shape JSON into Parquet or Iceberg, and see what landed."],
  runs: ["Runs", "Every run this console started: its phases, how it came out, and what it committed."],
  tables: ["Tables", "A table's history, read from its Iceberg metadata: every snapshot as an event."],
  inspect: ["Inspect", "Read back a Parquet file or an Iceberg table — schema and row count."],
};

function showView(name) {
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.toggle("active", n.dataset.view === name));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === "view-" + name));
  const [t, s] = TITLES[name] || TITLES.shape;
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

function renderTable(t) {
  const events = (t.events || []).slice();
  const rows = events.slice().reverse().map((e) => `
    <div class="ev${e.expired ? " gone" : ""}${tablesState.snap && e.snapshot_id === tablesState.snap ? " hl" : ""}">
      <span class="s">${e.sequence_number != null && e.snapshot_id ? "#" + e.sequence_number : "—"}</span>
      <span class="k ek-${esc(e.kind)}">${esc(e.kind)}</span>
      <span class="d">${eventWords(e)}${e.expired ? ' <span class="tag">expired</span>' : ""}</span>
      <span class="r">${[fmtTime(e.timestamp_ms), eventWriter(e)].filter(Boolean).join(" · ")}</span>
    </div>`).join("");
  const debt = t.delete_files && t.data_files ? Math.round((t.delete_files / t.data_files) * 100) : 0;
  $("table-detail").innerHTML = `
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
        <dt>read from</dt><dd>${t.versions_read} metadata version${t.versions_read === 1 ? "" : "s"}</dd>
      </dl>
    </div>`;
}

// ---- health -----------------------------------------------------------------

async function health() {
  try {
    const res = await fetch("/api/health").then((r) => r.json());
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
  toggleIcebergOpts();
  health();
  loadRuns();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
