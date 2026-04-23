"use strict";

const POLL_MS = 1000;

function tileClass(classify) {
  return `tile ${classify || "unknown"}`;
}

function fmt(v, digits = 2) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return v.toFixed(digits);
  return String(v);
}

// Build a span via DOM construction. Values go through textContent so
// strings that reach the dashboard from Redis (sensor names, status
// log messages, switch state names, etc.) cannot inject markup.
function makeSpan(className, text, style) {
  const s = document.createElement("span");
  if (className) s.className = className;
  if (style) s.setAttribute("style", style);
  s.textContent = text;
  return s;
}

async function fetchJson(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path} -> ${r.status}`);
  return r.json();
}

// ---- corr spectra ---------------------------------------------------

const magLayout = {
  margin: { l: 50, r: 20, t: 10, b: 40 },
  paper_bgcolor: "#1a1a1a",
  plot_bgcolor: "#111",
  font: { color: "#eee" },
  xaxis: { title: "Frequency (MHz)", gridcolor: "#333" },
  yaxis: { title: "Magnitude", type: "log", gridcolor: "#333" },
  showlegend: true,
  legend: { orientation: "h", y: -0.2 },
};

const phaseLayout = {
  margin: { l: 50, r: 20, t: 10, b: 40 },
  paper_bgcolor: "#1a1a1a",
  plot_bgcolor: "#111",
  font: { color: "#eee" },
  xaxis: { title: "Frequency (MHz)", gridcolor: "#333" },
  yaxis: { title: "Phase (rad)", range: [-Math.PI, Math.PI], gridcolor: "#333" },
  showlegend: true,
  legend: { orientation: "h", y: -0.2 },
};

let magPlotInitialized = false;
let phasePlotInitialized = false;

function updateCorr(corr) {
  if (!corr || !corr.pairs || !corr.freq_mhz) return;
  const freqs = corr.freq_mhz;
  const magTraces = [];
  const phaseTraces = [];
  for (const pair of Object.keys(corr.pairs)) {
    const p = corr.pairs[pair];
    if (!p) continue;
    if (p.mag) {
      magTraces.push({
        x: freqs,
        y: p.mag,
        type: "scatter",
        mode: "lines",
        name: pair,
        line: { width: 1.5 },
      });
    }
    if (p.phase) {
      phaseTraces.push({
        x: freqs,
        y: p.phase,
        type: "scatter",
        mode: "lines",
        name: pair,
        line: { width: 1.5 },
      });
    }
  }
  if (!magPlotInitialized) {
    Plotly.newPlot("plot-mag", magTraces, magLayout, { displayModeBar: false });
    magPlotInitialized = true;
  } else {
    Plotly.react("plot-mag", magTraces, magLayout, { displayModeBar: false });
  }

  if (phaseTraces.length > 0) {
    if (!phasePlotInitialized) {
      Plotly.newPlot("plot-phase", phaseTraces, phaseLayout, { displayModeBar: false });
      phasePlotInitialized = true;
    } else {
      Plotly.react("plot-phase", phaseTraces, phaseLayout, { displayModeBar: false });
    }
  }
}

// ---- health --------------------------------------------------------

function updateHealth(h, fileData) {
  const snapTile = document.getElementById("tile-snap");
  snapTile.className = `tile ${h.snap_connected ? "ok" : "danger"}`;
  snapTile.textContent = `SNAP: ${h.snap_connected ? "connected" : "offline"}`;

  const pandaTile = document.getElementById("tile-panda");
  let pandaClass = "danger";
  if (h.panda_heartbeat) pandaClass = "ok";
  else if (h.panda_connected) pandaClass = "warn";
  pandaTile.className = `tile ${pandaClass}`;
  pandaTile.textContent =
    `Panda: ${h.panda_heartbeat ? "alive" : h.panda_connected ? "stale HB" : "offline"}`;

  const obsTile = document.getElementById("tile-observing");
  obsTile.className = `tile ${h.observing_inferred ? "ok" : "warn"}`;
  obsTile.textContent = `Observing: ${h.observing_inferred ? "yes" : "no"}`;

  const fileTile = document.getElementById("tile-file");
  if (fileData) {
    fileTile.className = tileClass(fileData.classify);
    fileTile.textContent =
      fileData.seconds_since_write !== null
        ? `Last file: ${fmt(fileData.seconds_since_write, 0)}s ago`
        : "Last file: —";
  }
}

// ---- metadata + adc + tempctrl + rfswitch --------------------------

function renderMetadataTiles(meta) {
  const container = document.getElementById("metadata-tiles");
  container.replaceChildren();
  for (const sensor of Object.keys(meta).sort()) {
    const entry = meta[sensor];
    const row = document.createElement("div");
    row.className = "metadata-row";
    const classifyTag = Object.values(entry.classify || {})[0] || "unknown";
    const tileCls = tileClass(
      entry.status === "error" ? "danger" : classifyTag
    );
    const ageStr =
      entry.age_s !== null && entry.age_s !== undefined
        ? `${fmt(entry.age_s, 1)}s`
        : "—";
    row.append(
      makeSpan("label", sensor),
      makeSpan(tileCls, entry.status || "?"),
      makeSpan("value", ageStr)
    );
    container.appendChild(row);
  }
}

function renderTempctrlTiles(meta, classifiers) {
  const container = document.getElementById("tempctrl-tiles");
  container.replaceChildren();
  const tc = meta.tempctrl;
  if (!tc) {
    container.textContent = "no tempctrl data";
    return;
  }
  const value = tc.value || {};
  for (const chan of ["LNA", "LOAD"]) {
    const temp = value[`${chan}_T_now`];
    const drive = value[`${chan}_drive_level`];
    const tClass = (tc.classify || {})[`tempctrl.${chan}_T_now`] || "unknown";
    const dClass = (tc.classify || {})[`tempctrl.${chan}_drive_level`] || "unknown";
    const row = document.createElement("div");
    row.className = "tempctrl-row";
    row.append(
      makeSpan("label", chan),
      makeSpan(tileClass(tClass), `${fmt(temp, 2)} C`),
      makeSpan(tileClass(dClass), `drive ${fmt(drive, 2)}`)
    );
    container.appendChild(row);
  }
}

function renderAdcTiles(adc) {
  const container = document.getElementById("adc-tiles");
  container.replaceChildren();
  if (!adc.per_input) return;
  for (const entry of adc.per_input) {
    const row = document.createElement("div");
    row.className = "adc-row";
    const rmsStr = entry.rms !== null ? fmt(entry.rms, 1) : "—";
    const clipStr =
      entry.clip_frac !== null && entry.clip_frac !== undefined
        ? fmt(entry.clip_frac * 100, 2) + "%"
        : "—";
    row.append(
      makeSpan("label", `in${entry.input}/c${entry.core}`),
      makeSpan("value", `rms ${rmsStr}`),
      makeSpan("value", `clip ${clipStr}`)
    );
    container.appendChild(row);
  }
}

function renderRfswitch(rf) {
  const el = document.getElementById("rfswitch");
  el.replaceChildren();
  if (!rf.state) {
    el.textContent = "no switch data";
    return;
  }
  const cls = rf.on_schedule ? "ok" : "warn";
  const timeStr =
    rf.time_in_state_s !== null && rf.time_in_state_s !== undefined
      ? fmt(rf.time_in_state_s, 0) + "s"
      : "—";
  const nextStr =
    rf.next_expected_change_s !== null &&
    rf.next_expected_change_s !== undefined
      ? fmt(rf.next_expected_change_s, 0) + "s"
      : "—";

  const row1 = document.createElement("div");
  row1.className = "metadata-row";
  row1.append(
    makeSpan("label", "state"),
    makeSpan(tileClass(cls), rf.state),
    makeSpan("value", timeStr)
  );

  const row2 = document.createElement("div");
  row2.className = "metadata-row";
  row2.append(
    makeSpan("label", "next change"),
    // colspan is not valid on <span>; span across the remaining two
    // value columns via CSS grid instead.
    makeSpan("value", nextStr, "grid-column: span 2;")
  );

  el.append(row1, row2);
}

function renderStatusLog(entries) {
  const ul = document.getElementById("status-log");
  ul.replaceChildren();
  for (const entry of entries.slice(-100).reverse()) {
    const li = document.createElement("li");
    // Level is a number from the producer; coerce to a safe class token.
    const level = Number.isFinite(entry.level) ? entry.level : 20;
    li.className = `level-${level}`;
    const ts = new Date((entry.ts_unix || 0) * 1000).toISOString().slice(11, 19);
    li.textContent = `[${ts}] ${entry.msg}`;
    ul.appendChild(li);
  }
}

// ---- poll loop ------------------------------------------------------

async function tick() {
  try {
    const [health, corr, metadata, adc, rfswitch, file, status] = await Promise.all([
      fetchJson("/api/health"),
      fetchJson("/api/corr"),
      fetchJson("/api/metadata"),
      fetchJson("/api/adc"),
      fetchJson("/api/rfswitch"),
      fetchJson("/api/file"),
      fetchJson("/api/status"),
    ]);
    updateHealth(health.data, file.data);
    updateCorr(corr.data);
    renderMetadataTiles(metadata.data);
    renderTempctrlTiles(metadata.data, null);
    renderAdcTiles(adc.data);
    renderRfswitch(rfswitch.data);
    renderStatusLog(status.data);
  } catch (e) {
    console.error("poll failed:", e);
  }
}

tick();
setInterval(tick, POLL_MS);
