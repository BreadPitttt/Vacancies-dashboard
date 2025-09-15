// Government Jobs Dashboard — updated app.js
// Rules implemented here (frontend):
// - Load data.json and render cards
// - Filters via pill buttons
// - Mark "Applied" or "Not Interested" with localStorage
// - Expired jobs are hidden from the active list
//   - If a job expires after being marked Applied or Not Interested, it appears in an Archive section
// - XSS-safe text/attribute escaping

// -------------------- State --------------------
const state = {
  data: { jobListings: [], transparencyInfo: {} },
  filters: {
    urgency: new Set(),
    qual: new Set(),
    skill: new Set(),
    state: new Set(),
    source: new Set()
  }
};

// -------------------- Utilities --------------------
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function parseISO(d) {
  return d ? new Date(d) : null;
}

function daysLeft(deadlineStr) {
  const d = parseISO(deadlineStr);
  if (!d || Number.isNaN(d.getTime())) return null;
  const ms = d.setHours(23, 59, 59, 999) - Date.now();
  return Math.ceil(ms / (1000 * 60 * 60 * 24));
}

function isExpired(deadlineStr) {
  const d = parseISO(deadlineStr);
  if (!d || Number.isNaN(d.getTime())) return false;
  return d.setHours(23, 59, 59, 999) < Date.now();
}

function urgencyChip(deadlineStr) {
  const dl = daysLeft(deadlineStr);
  if (dl === null) return { cls: "chip", label: "Open" };
  if (dl < 0) return { cls: "chip chip--urgent", label: "Closed" };
  if (dl <= 7) return { cls: "chip chip--urgent", label: `Urgent · ${dl}d` };
  if (dl <= 15) return { cls: "chip chip--warn", label: `Soon · ${dl}d` };
  return { cls: "chip chip--ok", label: `Open · ${dl}d` };
}

function badgeForSource(src) {
  const s = (src || "").toLowerCase();
  if (s === "official") return "badge badge--official";
  if (s === "archive") return "badge badge--archive";
  if (s === "aggregator") return "badge badge--aggregator";
  return "badge";
}

function matchesFilters(job) {
  const F = state.filters;
  const inSetOrEmpty = (set, val) => set.size === 0 || set.has(val);

  if (!inSetOrEmpty(F.urgency, (job.urgency || "").toLowerCase())) return false;
  if (!inSetOrEmpty(F.qual, job.qualificationLevel || "")) return false;

  if (F.skill.size > 0) {
    const skills = (job.additionalSkills || []).map(String);
    for (const need of F.skill) if (!skills.includes(need)) return false;
  }

  if (!inSetOrEmpty(F.state, job.domicile || "")) return false;
  if (!inSetOrEmpty(F.source, (job.source || "").toLowerCase())) return false;
  return true;
}

// -------------------- Local storage (Applied / Hidden) --------------------
function appliedKey(id) { return `applied:${id}`; }
function hiddenKey(id)  { return `hide:${id}`; }
function isApplied(id) { try { return localStorage.getItem(appliedKey(id)) === "1"; } catch { return false; } }
function isHidden(id)  { try { return localStorage.getItem(hiddenKey(id))  === "1"; } catch { return false; } }
function setApplied(id, on) { try { on ? localStorage.setItem(appliedKey(id), "1") : localStorage.removeItem(appliedKey(id)); } catch {} }
function setHidden(id, on)  { try { on ? localStorage.setItem(hiddenKey(id),  "1") : localStorage.removeItem(hiddenKey(id)); } catch {} }

// -------------------- Escaping --------------------
function escapeHTML(s){
  return String(s).replace(/[&<>"']/g, (m) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;",
  }[m]));
}
function escapeAttr(s){ return escapeHTML(s); }

// -------------------- Rendering --------------------
function ensureArchiveSections(jobsRoot) {
  let archWrap = $("#archiveWrap");
  if (!archWrap) {
    archWrap = document.createElement("section");
    archWrap.id = "archiveWrap";
    archWrap.innerHTML = `
      <h2 style="margin-top:2rem">Review Archive</h2>
      <div id="appliedArchive" role="list" aria-label="Expired — Applied"></div>
      <div id="hiddenArchive" role="list" aria-label="Expired — Not Interested"></div>
    `;
    jobsRoot.parentNode.appendChild(archWrap);
  }
  return {
    applied: $("#appliedArchive"),
    hidden: $("#hiddenArchive")
  };
}

function cardHTML(j) {
  const chip = urgencyChip(j.deadline);
  const applied = isApplied(j.id);
  return `
    <h3>${escapeHTML(j.title || "")}</h3>
    <p>${escapeHTML(j.organization || "")}</p>
    <div class="badges" aria-label="Attributes">
      ${j.qualificationLevel ? `<span class="badge">${escapeHTML(j.qualificationLevel)}</span>` : ""}
      ${j.domicile ? `<span class="badge">${escapeHTML(j.domicile)}</span>` : ""}
      ${j.source ? `<span class="${badgeForSource(j.source)}">${escapeHTML(String(j.source).toUpperCase())}</span>` : ""}
    </div>
    <p><span class="${chip.cls}" aria-label="Deadline">${chip.label}</span></p>
    <div class="actions">
      ${j.applyLink ? `<a href="${escapeAttr(j.applyLink)}" target="_blank" rel="noopener"><button class="btn btn--primary">Apply Online</button></a>` : ""}
      ${j.pdfLink ? `<a href="${escapeAttr(j.pdfLink)}" target="_blank" rel="noopener"><button class="btn">View Notification</button></a>` : ""}
      <button class="btn js-applied" data-id="${escapeAttr(j.id)}" aria-pressed="${applied ? "true" : "false"}">${applied ? "Applied" : "Mark Applied"}</button>
      <button class="btn js-hide" data-id="${escapeAttr(j.id)}">Not Interested</button>
    </div>
  `;
}

function render() {
  const jobsRoot = $("#jobs");
  if (!jobsRoot) return;

  const { applied: appliedArch, hidden: hiddenArch } = ensureArchiveSections(jobsRoot);

  jobsRoot.innerHTML = "";
  appliedArch.innerHTML = "";
  hiddenArch.innerHTML = "";

  const all = (state.data.jobListings || [])
    .filter(j => !isHidden(j.id))
    .filter(matchesFilters);

  const active = all.filter(j => !isExpired(j.deadline));
  const expiredApplied = all.filter(j => isExpired(j.deadline) && isApplied(j.id));
  const expiredHidden  = all.filter(j => isExpired(j.deadline) && !isApplied(j.id) && isHidden(j.id));

  for (const j of active) {
    const card = document.createElement("article");
    card.className = "card";
    card.setAttribute("role", "listitem");
    card.innerHTML = cardHTML(j);
    jobsRoot.appendChild(card);
  }

  for (const j of expiredApplied) {
    const card = document.createElement("article");
    card.className = "card card--archive";
    card.setAttribute("role", "listitem");
    card.innerHTML = cardHTML(j);
    appliedArch.appendChild(card);
  }

  for (const j of expiredHidden) {
    const card = document.createElement("article");
    card.className = "card card--archive";
    card.setAttribute("role", "listitem");
    card.innerHTML = cardHTML(j);
    hiddenArch.appendChild(card);
  }

  const statusEl = $("#status");
  if (statusEl) {
    const last = state.data?.transparencyInfo?.lastUpdated || "—";
    statusEl.textContent = `Last updated: ${last} • Showing ${active.length}`;
  }
}

// -------------------- Filters + Actions --------------------
function initFilters() {
  document.addEventListener("click", (e) => {
    // Filter pills
    const pill = e.target.closest(".pill");
    if (pill) {
      const facet = pill.dataset.filter;
      const value = pill.dataset.value;
      if (facet && value) {
        pill.classList.toggle("pill--active");
        const on = pill.classList.contains("pill--active");
        pill.setAttribute("aria-pressed", on ? "true" : "false");
        const set = state.filters[facet];
        if (set) {
          on ? set.add(value) : set.delete(value);
          render();
        }
      } else if (pill && pill.id === "clearFilters") {
        for (const k of Object.keys(state.filters)) state.filters[k].clear();
        $$(".pill.pill--active").forEach(b => { b.classList.remove("pill--active"); b.setAttribute("aria-pressed", "false"); });
        render();
      }
    }

    // Applied toggle
    const applyBtn = e.target.closest(".js-applied");
    if (applyBtn) {
      const id = applyBtn.dataset.id;
      const next = !(applyBtn.getAttribute("aria-pressed") === "true");
      setApplied(id, next);
      applyBtn.setAttribute("aria-pressed", next ? "true" : "false");
      applyBtn.textContent = next ? "Applied" : "Mark Applied";
      render();
      return;
    }

    // Not Interested (hide)
    const hideBtn = e.target.closest(".js-hide");
    if (hideBtn) {
      const id = hideBtn.dataset.id;
      setHidden(id, true);
      render();
      return;
    }
  });
}

// -------------------- Data load --------------------
async function loadData() {
  const s = $("#status");
  try {
    const res = await fetch("data.json", { cache: "no-store" });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.data = await res.json();
    if (s) s.textContent = "Loaded.";
    render();
  } catch (err) {
    console.error(err);
    if (s) s.textContent = "Failed to load data.";
  }
}

// -------------------- Init --------------------
document.addEventListener("DOMContentLoaded", () => {
  initFilters();
  loadData();
});
