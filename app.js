// app.js — Cloudflare-ready, self-learning feedback

// Your Cloudflare Pages base
const CF_BASE = 'https://649b07cd.vacancies-dashboard.pages.dev';
const feedbackEndpoint = CF_BASE + '/feedback';

function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function escapeAttr(s){ return String(s||'').replace(/"/g,'&quot;'); }
const bust = () => `?t=${Date.now()}`;

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

async function renderJobs() {
  const mount = document.getElementById('jobs-root');
  if (!mount) return;
  try {
    const res = await fetch('data.json' + bust(), { cache: 'no-store' });
    if (!res.ok) throw new Error(`data.json HTTP ${res.status}`);
    const data = await res.json();
    const jobs = Array.isArray(data.jobListings) ? data.jobListings : [];

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
          <button class="btn success btn-vote" data-vote="right" data-id="${escapeAttr(job.id)}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}" title="This listing is correct">✔ Right</button>
          <button class="btn danger btn-vote" data-vote="wrong" data-id="${escapeAttr(job.id)}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}" title="This listing breaks policy">✖ Wrong</button>
        </p>
      `;
      mount.appendChild(card);
    });
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
        const txt = await r.text().catch(()=> '');
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
      const txt = await response.text().catch(()=> '');
      alert(`Failed to submit (HTTP ${response.status}).`);
      return;
    }
    alert('Thank you, your submission was received!');
    form.closest('.modal').classList.add('hidden');
    form.reset();
  } catch (error) {
    alert('An error occurred. Please try again.');
  }
}

document.addEventListener('DOMContentLoaded', () => {
  renderStatusBanner();
  renderJobs();
  setupFeedbackForms();
});
