// Minimal, accessible dashboard behavior with neo‑minimal UI

// -------------------- State --------------------
const state = {
  data: { jobListings: [], transparencyInfo: {} },
  filters: {
    urgency: new Set(),
    qual: new Set(),
    skill: new Set(),
    state: new Set(),
    source: new Set(),
  },
};

// -------------------- Utilities --------------------
const $ = (sel, root = document) => root.querySelector(sel);
const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

function parseISO(d) {
  // Accepts YYYY-MM-DD or ISO datetime
  return d ? new Date(d) : null;
}
function daysLeft(deadlineStr) {
  const d = parseISO(deadlineStr);
  if (!d || Number.isNaN(d.getTime())) return null;
  const ms = d.setHours(23, 59, 59, 999) - Date.now();
  return Math.ceil(ms / (1000 * 60 * 60 * 24));
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

// Local storage helpers
function appliedKey(id) { return `applied:${id}`; }
function hiddenKey(id)  { return `hide:${id}`; }
function isApplied(id) { try { return localStorage.getItem(appliedKey(id)) === "1"; } catch { return false; } }
function isHidden(id)  { try { return localStorage.getItem(hiddenKey(id))  === "1"; } catch { return false; } }
function setApplied(id, on) { try { on ? localStorage.setItem(appliedKey(id), "1") : localStorage.removeItem(appliedKey(id)); } catch {} }
function setHidden(id, on)  { try { on ? localStorage.setItem(hiddenKey(id),  "1") : localStorage.removeItem(hiddenKey(id)); } catch {} }

// -------------------- Rendering --------------------
function render() {
  const jobsRoot = $("#jobs");
  if (!jobsRoot) return;

  jobsRoot.innerHTML = "";
  const listings = (state.data.jobListings || [])
    .filter((j) => !isHidden(j.id))
    .filter(matchesFilters);

  for (const j of listings) {
    const chip = urgencyChip(j.deadline);
    const applied = isApplied(j.id);
    const card = document.createElement("article");
    card.className = "card";
    card.setAttribute("role", "listitem");
    card.innerHTML = `
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
    jobsRoot.appendChild(card);
  }

  const statusEl = $("#status");
  if (statusEl) {
    statusEl.textContent = `Last updated: ${state.data?.transparencyInfo?.lastUpdated || "—"} • Showing ${listings.length}`;
  }
}

// -------------------- Escaping --------------------
function escapeHTML(s) {
  return String(s).replace(/[&<>"']/g, (m) => ({
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    "\"": "&quot;",
    "'": "&#39;", // single-quote entity to avoid syntax issues
  }[m]));
}
function escapeAttr(s) { return escapeHTML(s); }

// -------------------- Filters --------------------
function initFilters() {
  document.addEventListener("click", (e) => {
    const pill = e.target.closest(".pill");
    if (pill) {
      const facet = pill.dataset.filter;   // from data-filter attribute
      const value = pill.dataset.value;    // from data-value attribute
      if (facet && value) {
        pill.classList.toggle("pill--active");
        const on = pill.classList.contains("pill--active");
        pill.setAttribute("aria-pressed", on ? "true" : "false");
        const set = state.filters[facet];
        if (set) {
          on ? set.add(value) : set.delete(value);
          render();
        }
      } else if (pill.id === "clearFilters") {
        for (const k of Object.keys(state.filters)) state.filters[k].clear();
        $$(".pill.pill--active").forEach((b) => {
          b.classList.remove("pill--active");
          b.setAttribute("aria-pressed", "false");
        });
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
      return;
    }

    // Hide
    const hideBtn = e.target.closest(".js-hide");
    if (hideBtn) {
      const id = hideBtn.dataset.id;
      setHidden(id, true);
      render();
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
