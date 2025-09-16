// app.js — Banner + placeholder for jobs rendering

async function renderStatusBanner() {
  const pill = document.getElementById('health-pill');
  const last = document.getElementById('last-updated');
  const total = document.getElementById('total-listings');

  try {
    const res = await fetch('health.json', { cache: 'no-store' });
    if (!res.ok) throw new Error('health.json not available');
    const h = await res.json();

    const ok = !!h.ok;
    pill.textContent = ok ? 'Health: OK' : 'Health: Not OK';
    pill.classList.remove('ok', 'bad');
    pill.classList.add(ok ? 'ok' : 'bad');

    // Prefer transparencyInfo fields; fall back to checkedAt
    const ts = h.lastUpdated || h.checkedAt || '';
    last.textContent = 'Last updated: ' + (ts || '—');

    const count = typeof h.totalListings === 'number' ? h.totalListings : '—';
    total.textContent = 'Listings: ' + count;
  } catch (e) {
    pill.textContent = 'Health: Unknown';
    pill.classList.remove('ok', 'bad');
    last.textContent = 'Last updated: —';
    total.textContent = 'Listings: —';
  }
}

// Placeholder: wire up existing data rendering if present.
// If your project already fetches data.json and renders it, leave that code as is.
async function renderJobsIfNeeded() {
  const mount = document.getElementById('jobs-root');
  if (!mount) return;
  // Example minimal verification without altering your existing UI logic:
  try {
    const res = await fetch('data.json', { cache: 'no-store' });
    if (!res.ok) return;
    const data = await res.json();
    // If you already have a renderer, call it here; otherwise keep placeholder:
    if (!Array.isArray(data.jobListings)) return;
    // No-op rendering to avoid conflicts; project-specific rendering lives elsewhere.
  } catch {}
}

document.addEventListener('DOMContentLoaded', () => {
  renderStatusBanner();
  renderJobsIfNeeded();
});
