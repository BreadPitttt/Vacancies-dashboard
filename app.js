// app.js — Cloudflare + personal tracker + self-learning

const CF_BASE = 'https://649b07cd.vacancies-dashboard.pages.dev';
const feedbackEndpoint = CF_BASE + '/feedback';

function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function escapeAttr(s){ return String(s||'').replace(/"/g,'&quot;'); }
const bust = () => `?t=${Date.now()}`;

function parseDateMaybe(s){
  if (!s || s.toUpperCase()==='N/A') return null;
  const fmts = ["YYYY-MM-DD","DD/MM/YYYY","DD-MM-YYYY"];
  // naive parse
  const m = s.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (m) return new Date(+m[1], +m[2]-1, +m[3]);
  const m2 = s.match(/^(\d{2})[\/-](\d{2})[\/-](\d{4})$/);
  if (m2) return new Date(+m2[3], +m2[2]-1, +m2[1]);
  return null;
}

let GLOBAL_JOBS = [];
let GLOBAL_STATE = {}; // { jobId: {action:'applied'|'not_interested', ts}}

async function fetchState(){
  try{
    const res = await fetch('user_state.json' + bust(), { cache:'no-store' });
    if (res.ok) GLOBAL_STATE = await res.json();
  }catch(_){}
}

async function renderStatusBanner() {
  const pill = document.getElementById('health-pill');
  const last = document.getElementById('last-updated');
  const total = document.getElementById('total-listings');
  try {
    const res = await fetch('health.json' + bust(), { cache: 'no-store' });
    if (!res.ok) throw new Error(`health.json HTTP ${res.status}`);
    const h = await res.json();
    const ok = !!h.ok;
    pill.textContent = ok ? 'Health: OK' : 'Health: Not OK';
    pill.className = ok ? 'pill ok' : 'pill bad';
    const ts = h.lastUpdated || h.checkedAt || h.lastChecked || '';
    last.textContent = 'Last updated: ' + (ts ? new Date(ts).toLocaleString() : '—');
    const count = typeof h.totalListings === 'number' ? h.totalListings : '—';
    total.textContent = 'Listings: ' + count;
  } catch (e) {
    pill.textContent = 'Health: Unknown';
    pill.className = 'pill';
    last.textContent = 'Last updated: —';
    total.textContent = 'Listings: —';
  }
}

function renderSections(){
  const jobs = GLOBAL_JOBS;
  const now = new Date();
  const appliedRoot = document.getElementById('applied-root');
  const notintRoot = document.getElementById('notint-root');
  if (appliedRoot) appliedRoot.innerHTML = '';
  if (notintRoot) notintRoot.innerHTML = '';

  const byId = {};
  jobs.forEach(j => { byId[j.id] = j; });

  const appliedList = [];
  const notInterestedList = [];

  for (const j of jobs){
    const st = GLOBAL_STATE[j.id];
    const deadline = parseDateMaybe(j.deadline);
    const deadlinePassed = deadline ? (deadline < now) : false;

    if (st && st.action === 'applied') {
      appliedList.push(j);
    } else if (st && st.action === 'not_interested') {
      // auto-expire 15 days after timestamp
      const ts = new Date(st.ts || now);
      const age = (now - ts)/(1000*60*60*24);
      if (age <= 15) notInterestedList.push(j);
    } else if (!st && deadlinePassed) {
      // No action; move to not interested view if > 0 days past
      notInterestedList.push(j);
    }
  }

  function miniCard(j, kind){
    return `
      <div class="job-mini">
        <span>${escapeHtml(j.title||'')}</span>
        <div class="row">
          <button class="btn small btn-undo" data-id="${escapeAttr(j.id)}">Move back</button>
          ${kind==='applied' ? '' : `<a class="btn small" href="${j.applyLink}" target="_blank" rel="noopener">Apply</a>`}
        </div>
      </div>
    `;
  }

  if (appliedRoot) {
    appliedRoot.innerHTML = appliedList.length ? appliedList.map(j => miniCard(j,'applied')).join('') : '<p>No applied items yet.</p>';
  }
  if (notintRoot) {
    notintRoot.innerHTML = notInterestedList.length ? notInterestedList.map(j => miniCard(j,'notint')).join('') : '<p>No not-interested items currently.</p>';
  }
}

async function renderJobs() {
  const mount = document.getElementById('jobs-root');
  if (!mount) return;
  try {
    const res = await fetch('data.json' + bust(), { cache: 'no-store' });
    if (!res.ok) throw new Error(`data.json HTTP ${res.status}`);
    const data = await res.json();
    const jobs = Array.isArray(data.jobListings) ? data.jobListings : [];
    GLOBAL_JOBS = jobs;

    if (jobs.length === 0) {
      mount.innerHTML = '<p>No active job listings found.</p>';
      return;
    }
    mount.innerHTML = '';
    jobs.forEach(job => {
      if (job.flags && job.flags.hidden) return;
      const card = document.createElement('div');
      card.className = 'job-card';
      const daysLeft = (job.daysLeft ?? '—');
      card.innerHTML = `
        <h3>${escapeHtml(job.title || 'No Title')}</h3>
        <p><strong>Organization:</strong> ${escapeHtml(job.organization || 'N/A')}</p>
        <p><strong>Qualification:</strong> ${escapeHtml(job.qualificationLevel || 'N/A')}</p>
        <p><strong>Domicile:</strong> ${escapeHtml(job.domicile || 'All India')}</p>
        <p><strong>Deadline:</strong> ${escapeHtml(job.deadline || 'N/A')} (${daysLeft} days left)</p>
        <p class="links">
          <a class="btn primary" href="${job.applyLink}" target="_blank" rel="noopener">Apply Here</a>
          <button class="btn btn-report" data-id="${escapeAttr(job.id)}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}">Report</button>
          <span class="spacer"></span>
          <button class="btn success btn-vote" data-vote="right" data-id="${escapeAttr(job.id)}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}">✔ Right</button>
          <button class="btn danger btn-vote" data-vote="wrong" data-id="${escapeAttr(job.id)}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}">✖ Wrong</button>
        </p>
        <p class="links">
          <button class="btn outline btn-applied" data-id="${escapeAttr(job.id)}">Applied</button>
          <button class="btn outline btn-skip" data-id="${escapeAttr(job.id)}">Not Interested</button>
        </p>
      `;
      mount.appendChild(card);
    });

    // After rendering jobs, also render personal sections
    renderSections();

  } catch (e) {
    mount.innerHTML = `<p style="color: red;">Error loading job listings: ${escapeHtml(e.message)}</p>`;
  }
}

function setupFeedbackForms() {
  document.addEventListener('click', function(e) {
    if (e.target && e.target.classList.contains('btn-report')) {
      const listingId = e.target.getAttribute('data-id') || '';
      document.getElementById('reportListingId').value = listingId;
      document.getElementById('report-modal').classList.remove('hidden');
    }
  });

  const showMissingBtn = document.getElementById('btn-show-missing-form');
  if (showMissingBtn) {
    showMissingBtn.addEventListener('click', () => {
      document.getElementById('missing-modal').classList.remove('hidden');
    });
  }

  document.querySelectorAll('.modal button[type="button"]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.target.closest('.modal').classList.add('hidden');
    });
  });

  const reportForm = document.getElementById('reportForm');
  if (reportForm) reportForm.addEventListener('submit', submitForm);

  const missingForm = document.getElementById('missingForm');
  if (missingForm) missingForm.addEventListener('submit', submitForm);

  // Votes
  document.addEventListener('click', async function(e){
    const t = e.target;
    if (!t || !t.classList.contains('btn-vote')) return;
    const vote = t.getAttribute('data-vote');
    const jobId = t.getAttribute('data-id') || '';
    const title = t.getAttribute('data-title') || '';
    const url = t.getAttribute('data-url') || '';
    t.disabled = true;
    try{
      const payload = { jobId, title, url, flag: vote === 'wrong' ? 'not general vacancy' : 'right' };
      const r = await fetch(feedbackEndpoint, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ type: 'report', payload })
      });
      if (!r.ok){
        alert(`Failed to submit (HTTP ${r.status}).`);
        t.disabled = false;
        return;
      }
      t.textContent = vote === 'wrong' ? 'Reported ✖' : 'Thanks ✔';
    }catch(_){
      t.disabled = false;
      alert('An error occurred. Please try again.');
    }
  });

  // Personal state
  document.addEventListener('click', async function(e){
    const t = e.target;
    if (!t) return;
    const jid = t.getAttribute('data-id');
    if (t.classList.contains('btn-applied') || t.classList.contains('btn-skip') || t.classList.contains('btn-undo')) {
      const action = t.classList.contains('btn-undo') ? 'undo' : (t.classList.contains('btn-applied') ? 'applied' : 'not_interested');
      try{
        const r = await fetch(feedbackEndpoint, {
          method:'POST',
          headers:{'Content-Type':'application/json'},
          body: JSON.stringify({ type: 'state', payload: { jobId: jid, action, ts: new Date().toISOString() } })
        });
        if (!r.ok) { alert(`Failed to save (HTTP ${r.status}).`); return; }
        // refresh state and re-render sections
        await fetchState();
        renderSections();
      }catch(_){
        alert('Error saving state.');
      }
    }
  });
}

async function submitForm(e){
  e.preventDefault();
  const form = e.target;
  const formType = form.id === 'reportForm' ? 'report' : 'missing';
  let payload;

  if (formType === 'report') {
    payload = {
      listingId: document.getElementById('reportListingId').value,
      reason: (document.getElementById('reportReason').value || '').trim(),
      evidenceUrl: (document.getElementById('reportEvidenceUrl').value || '').trim() || null,
      note: (document.getElementById('reportNote').value || '').trim() || null
    };
  } else {
    payload = {
      title: (document.getElementById('missingTitle').value || '').trim(),
      url: (document.getElementById('missingUrl').value || '').trim(),
      note: (document.getElementById('missingNote').value || '').trim() || null
    };
    if (!payload.title || !payload.url) {
      alert('Title and URL are required.');
      return;
    }
  }

  try {
    const response = await fetch(feedbackEndpoint, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ type: formType, payload })
    });
    if (!response.ok) {
      alert(`Failed to submit (HTTP ${response.status}).`);
      return;
    }
    alert('Saved!');
    form.closest('.modal').classList.add('hidden');
    form.reset();
  } catch (_error) {
    alert('An error occurred. Please try again.');
  }
}

document.addEventListener('DOMContentLoaded', async () => {
  await fetchState();
  await renderStatusBanner();
  await renderJobs();
  setupFeedbackForms();
});
