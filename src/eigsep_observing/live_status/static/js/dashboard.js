"use strict";

const POLL_MS = 1000;
const WIRING_TOGGLE_KEY = "eigsep.useWiringLabels";
const CALIBRATED_TOGGLE_KEY = "eigsep.useCalibrated";
const VNA_MODE_KEY = "eigsep.vnaMode";

// Wiring-labels toggle. Persisted in localStorage so the user's
// choice survives reloads; gracefully no-ops when no labels are
// present in the API payload (e.g. lab benches with no wiring).
function getUseWiringLabels() {
  try {
    return localStorage.getItem(WIRING_TOGGLE_KEY) !== "0";
  } catch (e) {
    return true;
  }
}

function setUseWiringLabels(on) {
  try {
    localStorage.setItem(WIRING_TOGGLE_KEY, on ? "1" : "0");
  } catch (e) {
    // localStorage unavailable (e.g. private mode); checkbox state still works.
  }
}

// Calibrated-spectra toggle. Default off — operators flip it on when
// they want spectra in Kelvin via the first-order Y-factor cal.
function getUseCalibrated() {
  try {
    return localStorage.getItem(CALIBRATED_TOGGLE_KEY) === "1";
  } catch (e) {
    return false;
  }
}

function setUseCalibrated(on) {
  try {
    localStorage.setItem(CALIBRATED_TOGGLE_KEY, on ? "1" : "0");
  } catch (e) {
    // see setUseWiringLabels
  }
}

// VNA pane mode (antenna or receiver). Persisted so the operator's
// choice survives reloads. Defaults to "ant" — that's the
// science-interesting one.
function getVnaMode() {
  try {
    const v = localStorage.getItem(VNA_MODE_KEY);
    return v === "rec" ? "rec" : "ant";
  } catch (e) {
    return "ant";
  }
}

function setVnaMode(mode) {
  try {
    localStorage.setItem(VNA_MODE_KEY, mode === "rec" ? "rec" : "ant");
  } catch (e) {
    // see setUseWiringLabels
  }
}

// Pick wiring label when the toggle is on AND the API gave us one;
// otherwise fall back to the raw key the caller passed in (input
// number, pair string, etc.).
function pickLabel(rawKey, apiLabel) {
  if (getUseWiringLabels() && apiLabel) return apiLabel;
  return rawKey;
}

function tileClass(classify) {
  return `tile ${classify || "unknown"}`;
}

function fmt(v, digits = 2) {
  if (v === null || v === undefined) return "—";
  if (typeof v === "number") return v.toFixed(digits);
  return String(v);
}

// Human-friendly duration for a non-negative number of seconds.
// Picks the two largest non-zero units so steady-state observing
// (which can run for days) doesn't render as a six-digit second count.
function fmtDuration(seconds) {
  if (seconds === null || seconds === undefined) return "—";
  const s = Math.max(0, Math.floor(seconds));
  if (s < 60) return `${s}s`;
  const m = Math.floor(s / 60);
  if (m < 60) return `${m}m ${s % 60}s`;
  const h = Math.floor(m / 60);
  if (h < 24) return `${h}h ${m % 60}m`;
  const d = Math.floor(h / 24);
  return `${d}d ${h % 24}h`;
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

// Plotly log axes take ranges in log10. [-2, 9] mirrors the legacy
// live_plotter ylim of (1e-2, 1e9); fixed range avoids autoscale jitter.
// Calibrated mode swaps to a linear y-axis in Kelvin and lets plotly
// autorange — sky / load / on temperatures span ~1.5 orders of
// magnitude, well within plotly's autorange comfort zone.
const magLayoutRaw = {
  margin: { l: 50, r: 20, t: 10, b: 40 },
  paper_bgcolor: "#1a1a1a",
  plot_bgcolor: "#111",
  font: { color: "#eee" },
  xaxis: { title: "Frequency [MHz]", gridcolor: "#333" },
  yaxis: {
    title: "Amplitude [arb. units]",
    type: "log",
    range: [-2, 9],
    gridcolor: "#333",
  },
  showlegend: true,
  legend: { orientation: "h", y: -0.2 },
};
const magLayoutCal = {
  ...magLayoutRaw,
  yaxis: {
    title: "Temperature [K]",
    type: "linear",
    autorange: true,
    gridcolor: "#333",
  },
};

const phaseLayout = {
  margin: { l: 50, r: 20, t: 10, b: 40 },
  paper_bgcolor: "#1a1a1a",
  plot_bgcolor: "#111",
  font: { color: "#eee" },
  xaxis: { title: "Frequency [MHz]", gridcolor: "#333" },
  yaxis: {
    title: "Phase [deg]",
    range: [-180, 180],
    gridcolor: "#333",
  },
  showlegend: true,
  legend: { orientation: "h", y: -0.2 },
};

const RAD_TO_DEG = 180 / Math.PI;

let magPlotInitialized = false;
let phasePlotInitialized = false;

// Render the cal status bar above the plots. Three states:
//   - toggle off → hidden
//   - toggle on, cal unavailable → red "Calibration unavailable" warn bar
//   - toggle on, cal applied → green info bar with on/off cache age
// We deliberately show the cache age unconditionally rather than gating
// on a freshness threshold: ``RFANT`` dwells for an hour, so any threshold
// either rejects nearly every antenna integration or is so loose it adds
// nothing. The "switch has stopped cycling" failure mode is surfaced
// separately by the rfswitch ``on_schedule`` tile.
function renderCalibrationBar(meta) {
  const el = document.getElementById("calibration-bar");
  if (!el) return;
  if (!getUseCalibrated() || !meta) {
    el.hidden = true;
    el.className = "cal-bar";
    el.textContent = "";
    return;
  }
  el.hidden = false;
  if (meta.stale) {
    el.className = "cal-bar warn";
    el.textContent = `Calibration unavailable — showing raw. Reason: ${meta.reason || "unknown"}`;
    return;
  }
  const ages = [meta.last_rfnoff_age_s, meta.last_rfnon_age_s].filter(
    (v) => v !== null && v !== undefined
  );
  const ageStr = ages.length ? fmtDuration(Math.max(...ages)) : "—";
  const gainStr =
    meta.gain_median !== null && meta.gain_median !== undefined
      ? fmt(meta.gain_median, 3)
      : "—";
  el.className = "cal-bar info";
  el.textContent = `Calibrated — on/off cache ${ageStr} old, gain median ${gainStr}`;
}

function updateCorr(corr) {
  if (!corr || !corr.pairs || !corr.freq_mhz) return;
  const freqs = corr.freq_mhz;
  const magTraces = [];
  const phaseTraces = [];
  for (const pair of Object.keys(corr.pairs)) {
    const p = corr.pairs[pair];
    if (!p) continue;
    const name = pickLabel(pair, p.label);
    if (p.mag) {
      magTraces.push({
        x: freqs,
        y: p.mag,
        type: "scatter",
        mode: "lines",
        name,
        line: { width: 1.5 },
      });
    }
    if (p.phase) {
      phaseTraces.push({
        x: freqs,
        y: p.phase.map((v) => v * RAD_TO_DEG),
        type: "scatter",
        mode: "lines",
        name,
        line: { width: 1.5 },
      });
    }
  }
  // Pick the y-axis layout based on whether the response is actually
  // calibrated. The server sets ``calibration_meta.stale=true`` and
  // returns raw values in the fallback case, so we look at meta + the
  // toggle to decide which axis to render.
  const isCalibrated =
    getUseCalibrated() && corr.calibration_meta && !corr.calibration_meta.stale;
  const layout = isCalibrated ? magLayoutCal : magLayoutRaw;
  if (!magPlotInitialized) {
    Plotly.newPlot("plot-mag", magTraces, layout, { displayModeBar: false });
    magPlotInitialized = true;
  } else {
    Plotly.react("plot-mag", magTraces, layout, { displayModeBar: false });
  }

  if (phaseTraces.length > 0) {
    if (!phasePlotInitialized) {
      Plotly.newPlot("plot-phase", phaseTraces, phaseLayout, { displayModeBar: false });
      phasePlotInitialized = true;
    } else {
      Plotly.react("plot-phase", phaseTraces, phaseLayout, { displayModeBar: false });
    }
  }
  renderCalibrationBar(corr.calibration_meta);
}

// ---- vna ------------------------------------------------------------

const vnaLayout = {
  margin: { l: 50, r: 20, t: 10, b: 40 },
  paper_bgcolor: "#1a1a1a",
  plot_bgcolor: "#111",
  font: { color: "#eee" },
  xaxis: { title: "Frequency [MHz]", gridcolor: "#333" },
  yaxis: {
    title: "|S11| [dB]",
    autorange: true,
    gridcolor: "#333",
  },
  showlegend: false,
};

let vnaPlotInitialized = false;

function renderVnaStatus(vna) {
  const el = document.getElementById("vna-status");
  if (!el) return;
  if (!vna) {
    el.className = "vna-status error";
    el.textContent = "VNA pane unavailable.";
    return;
  }
  if (!vna.available) {
    el.className = "vna-status";
    if (vna.reason === "calibration_failed") {
      el.className = "vna-status error";
      el.textContent =
        `Calibration failed for ${vna.mode}. Check server logs and ` +
        "OSL standards.";
    } else if (vna.reason === "no measurement received yet") {
      el.textContent = `No ${vna.mode} measurement received yet.`;
    } else {
      el.textContent = `No data: ${vna.reason || "unknown"}.`;
    }
    return;
  }
  const ageStr = fmtDuration(vna.age_s);
  const modeLabel = vna.mode === "ant" ? "Antenna" : "Receiver";
  if (vna.stale) {
    el.className = "vna-status stale";
    el.textContent =
      `${modeLabel} S11 — ${ageStr} old (stale; producer cadence is ~1/hour).`;
  } else {
    el.className = "vna-status";
    el.textContent = `${modeLabel} S11 — ${ageStr} old.`;
  }
}

function updateVna(vna) {
  renderVnaStatus(vna);
  if (!vna || !vna.available) {
    // Clear the plot so the operator doesn't see a stale trace under
    // an "unavailable" status banner.
    if (vnaPlotInitialized) {
      Plotly.react("plot-vna", [], vnaLayout, { displayModeBar: false });
    }
    return;
  }
  const traces = [
    {
      x: vna.freqs_mhz,
      y: vna.s11_db,
      type: "scatter",
      mode: "lines",
      line: { width: 1.5 },
    },
  ];
  if (!vnaPlotInitialized) {
    Plotly.newPlot("plot-vna", traces, vnaLayout, { displayModeBar: false });
    vnaPlotInitialized = true;
  } else {
    Plotly.react("plot-vna", traces, vnaLayout, { displayModeBar: false });
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

  const runTile = document.getElementById("tile-run");
  if (h.run_tag === null || h.run_tag === undefined) {
    runTile.className = "tile unknown";
    runTile.textContent = "Run: idle";
  } else if (h.run_tag === "UNKNOWN") {
    // Sentinel value from a malformed/partial publish; treat as a
    // misconfiguration signal rather than steady state.
    runTile.className = "tile warn";
    runTile.textContent = "Run: unknown";
  } else {
    runTile.className = "tile ok";
    const ageStr = fmtDuration(h.run_age_s);
    runTile.textContent = `Run: ${h.run_tag} (${ageStr})`;
  }

  const reinitTile = document.getElementById("tile-reinit");
  const reinit = h.snap_reinit || {};
  // No "ok" / "warn" classification: the count is informational, the
  // operator interprets it. A high count overnight means "SNAP was
  // thermal-cycling"; the dashboard surfaces it without judging.
  if (reinit.count === null || reinit.count === undefined) {
    reinitTile.className = "tile unknown";
    reinitTile.textContent = "Reinits: —";
  } else {
    reinitTile.className = "tile";
    const ageStr =
      reinit.seconds_since_reinit !== null &&
      reinit.seconds_since_reinit !== undefined
        ? ` (${fmt(reinit.seconds_since_reinit, 0)}s ago)`
        : "";
    reinitTile.textContent = `Reinits: ${reinit.count}${ageStr}`;
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
  const channels = [
    { label: "LNA", stream: "tempctrl_lna" },
    { label: "LOAD", stream: "tempctrl_load" },
  ];
  let any = false;
  for (const { label, stream } of channels) {
    const entry = meta[stream];
    if (!entry) continue;
    any = true;
    const value = entry.value || {};
    const tClass = (entry.classify || {})[`${stream}.T_now`] || "unknown";
    const dClass = (entry.classify || {})[`${stream}.drive_level`] || "unknown";
    const row = document.createElement("div");
    row.className = "tempctrl-row";
    row.append(
      makeSpan("label", label),
      makeSpan(tileClass(tClass), `${fmt(value.T_now, 2)} C`),
      makeSpan(tileClass(dClass), `drive ${fmt(value.drive_level, 2)}`)
    );
    container.appendChild(row);
  }
  if (!any) {
    container.textContent = "no tempctrl data";
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
    const inputName = pickLabel(`in${entry.input}`, entry.label);
    row.append(
      makeSpan("label", `${inputName}/c${entry.core}`),
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
  // on_schedule is tri-state: true = ok, false = warn, null = undefined
  // (no schedule in Redis, no observed transition yet, or panda
  // heartbeat dead). Treat null as neutral, not a failure.
  let cls;
  if (rf.on_schedule === true) cls = "ok";
  else if (rf.on_schedule === false) cls = "warn";
  else cls = "unknown";
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

// Last successful payloads cached so the wiring-labels toggle can
// re-render immediately without waiting up to POLL_MS for the next tick.
let lastCorr = null;
let lastAdc = null;

async function tick() {
  try {
    const corrPath = getUseCalibrated() ? "/api/corr?calibrated=1" : "/api/corr";
    const vnaPath = `/api/vna?mode=${encodeURIComponent(getVnaMode())}`;
    const [health, corr, metadata, adc, rfswitch, file, status, vna] = await Promise.all([
      fetchJson("/api/health"),
      fetchJson(corrPath),
      fetchJson("/api/metadata"),
      fetchJson("/api/adc"),
      fetchJson("/api/rfswitch"),
      fetchJson("/api/file"),
      fetchJson("/api/status"),
      fetchJson(vnaPath),
    ]);
    lastCorr = corr.data;
    lastAdc = adc.data;
    updateHealth(health.data, file.data);
    updateCorr(corr.data);
    renderMetadataTiles(metadata.data);
    renderTempctrlTiles(metadata.data, null);
    renderAdcTiles(adc.data);
    renderRfswitch(rfswitch.data);
    renderStatusLog(status.data);
    updateVna(vna.data);
  } catch (e) {
    console.error("poll failed:", e);
  }
}

function initWiringToggle() {
  const cb = document.getElementById("toggle-wiring");
  if (!cb) return;
  cb.checked = getUseWiringLabels();
  cb.addEventListener("change", () => {
    setUseWiringLabels(cb.checked);
    if (lastCorr) updateCorr(lastCorr);
    if (lastAdc) renderAdcTiles(lastAdc);
  });
}

function initCalibratedToggle() {
  const cb = document.getElementById("toggle-calibrated");
  if (!cb) return;
  cb.checked = getUseCalibrated();
  // On change, fire an immediate refetch — the cal math runs server-side
  // so we need a fresh /api/corr response with the right query param.
  cb.addEventListener("change", () => {
    setUseCalibrated(cb.checked);
    tick();
  });
}

function initVnaModeToggle() {
  const buttons = document.querySelectorAll(".vna-mode-btn");
  if (!buttons.length) return;
  const apply = (mode) => {
    for (const btn of buttons) {
      btn.classList.toggle("active", btn.dataset.vnaMode === mode);
    }
  };
  apply(getVnaMode());
  for (const btn of buttons) {
    btn.addEventListener("click", () => {
      const mode = btn.dataset.vnaMode === "rec" ? "rec" : "ant";
      setVnaMode(mode);
      apply(mode);
      // Refetch immediately so the swap doesn't wait for the next poll.
      tick();
    });
  }
}

initWiringToggle();
initCalibratedToggle();
initVnaModeToggle();
tick();
setInterval(tick, POLL_MS);
