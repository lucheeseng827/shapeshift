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
function money(n) {
  const v = Number(n) || 0;
  return "$" + v.toLocaleString("en-US", { minimumFractionDigits: 2, maximumFractionDigits: 2 });
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
  inspect: ["Inspect", "Read back a Parquet file or an Iceberg table — schema and row count."],
  cost: ["Cost", "Turn a row count into the vendor-vs-self-host comparison."],
};

function showView(name) {
  document.querySelectorAll(".nav-item").forEach((n) => n.classList.toggle("active", n.dataset.view === name));
  document.querySelectorAll(".view").forEach((v) => v.classList.toggle("active", v.id === "view-" + name));
  const [t, s] = TITLES[name] || TITLES.shape;
  $("view-title").textContent = t;
  $("view-sub").textContent = s;
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
    toast(`Shaped ${fmtNum(res.rows_out)} rows → ${baseName(res.output_path)}`, "ok");
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
  $("result-hint").textContent = `${res.format} · ${fmtBytes(res.bytes)}`;
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

// ---- cost -------------------------------------------------------------------

async function doCost() {
  const rows = parseInt(String($("cost-rows").value).replace(/[^0-9]/g, ""), 10) || 0;
  const vendor = parseFloat($("cost-vendor").value) || 0;
  const self = parseFloat($("cost-selfhost").value) || 0;
  try {
    const res = await api("/api/cost", { rows, vendor_per_million: vendor, self_host_cost: self });
    const positive = res.saved >= 0;
    const pct = res.saved_fraction == null ? "" : ` (${(res.saved_fraction * 100).toFixed(1)}%)`;
    $("cost-result").innerHTML = `
      <div class="kpis" style="margin-top:14px">
        <div class="kpi"><div class="label">Vendor cost</div><div class="value">${money(res.vendor_cost)}</div><div class="foot">@ ${money(res.vendor_per_million)}/million MAR</div></div>
        <div class="kpi"><div class="label">Self-host cost</div><div class="value">${money(res.self_host_cost)}</div><div class="foot">${fmtNum(res.rows)} rows shaped</div></div>
        <div class="kpi ${positive ? "good" : "danger"}"><div class="label">Saved</div><div class="value">${money(res.saved)}</div><div class="foot">${positive ? "net saving" : "too small to amortize"}${pct}</div></div>
      </div>`;
  } catch (e) {
    toast(e.message, "err");
  }
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

// ---- wire up ----------------------------------------------------------------

function toggleIcebergOpts() {
  $("iceberg-opts").classList.toggle("hidden", $("out-format").value !== "iceberg");
}

function init() {
  document.querySelectorAll(".nav-item").forEach((n) => n.addEventListener("click", () => showView(n.dataset.view)));
  $("btn-sample").addEventListener("click", () => { $("src-input").value = SAMPLE; toast("Sample loaded — 4 good records + 1 bad line.", "ok"); });
  $("btn-clear-input").addEventListener("click", () => { $("src-input").value = ""; });
  $("btn-infer").addEventListener("click", doInfer);
  $("btn-shape").addEventListener("click", doShape);
  $("btn-inspect").addEventListener("click", doInspect);
  $("btn-cost").addEventListener("click", doCost);
  $("out-format").addEventListener("change", toggleIcebergOpts);
  toggleIcebergOpts();
  health();
}

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", init);
} else {
  init();
}
