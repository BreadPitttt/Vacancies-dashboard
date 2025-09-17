// app.js — Complete Final Version (backward-compatible)
const feedbackEndpoint = 'https://phenomenal-cat-7d2563.netlify.app/.netlify/functions/feedback';

// Health banner
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
    pill.className = ok ? 'ok' : 'bad';
    const ts = h.lastUpdated || h.checkedAt || h.lastChecked || '';
    last.textContent = 'Last updated: ' + (ts ? new Date(ts).toLocaleString() : '—');
    const count = typeof h.totalActive === 'number' ? h.totalActive : '—';
    total.textContent = 'Listings: ' + count;
  } catch (e) {
    pill.textContent = 'Health: Unknown';
    last.textContent = 'Last updated: —';
    total.textContent = 'Listings: —';
  }
}

// Jobs list
async function renderJobs() {
  const mount = document.getElementById('jobs-root');
  if (!mount) return;
  try {
    const res = await fetch('data.json', { cache: 'no-store' });
    if (!res.ok) throw new Error('data.json not found');
    const data = await res.json();

    if (!Array.isArray(data.jobListings) || data.jobListings.length === 0) {
      mount.innerHTML = '<p>No active job listings found.</p>';
      return;
    }
    mount.innerHTML = '';
    data.jobListings.forEach(job => {
      if (job.flags && job.flags.hidden) return;
      const card = document.createElement('div');
      card.className = 'job-card';
      const daysLeft = job.daysLeft !== null && job.daysLeft !== undefined ? job.daysLeft : '—';
      card.innerHTML = `
        <h3>${escapeHtml(job.title || 'No Title')}</h3>
        <p><strong>Organization:</strong> ${escapeHtml(job.organization || 'N/A')}</p>
        <p><strong>Qualification:</strong> ${escapeHtml(job.qualificationLevel || 'N/A')}</p>
        <p><strong>Domicile:</strong> ${escapeHtml(job.domicile || 'All India')}</p>
        <p><strong>Deadline:</strong> ${escapeHtml(job.deadline || 'N/A')} (${daysLeft} days left)</p>
        <p class="links">
          <a class="btn primary" href="${job.applyLink}" target="_blank" rel="noopener">Apply Here</a>
          <button class="btn btn-report" data-id="${job.id}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}">Report</button>
          <span class="spacer"></span>
          <button class="btn success btn-vote" data-vote="right" data-id="${job.id}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}" title="This listing is correct">✔ Right</button>
          <button class="btn danger btn-vote" data-vote="wrong" data-id="${job.id}" data-title="${escapeAttr(job.title)}" data-url="${escapeAttr(job.applyLink)}" title="This listing breaks policy">✖ Wrong</button>
        </p>
      `;
      mount.appendChild(card);
    });
  } catch (e) {
    mount.innerHTML = `<p style="color: red;">Error loading job listings: ${escapeHtml(e.message)}</p>`;
  }
}

function setupFeedbackForms() {
  // Report modal opening
  document.addEventListener('click', function(e) {
    if (e.target && e.target.classList.contains('btn-report')) {
      const listingId = e.target.getAttribute('data-id') || '';
      document.getElementById('reportListingId').value = listingId;
      document.getElementById('report-modal').style.display = 'block';
    }
  });
  // Vote buttons (Right/Wrong) — maps to existing 'report' route
  document.addEventListener('click', async function(e){
    const t = e.target;
    if (!t || !t.classList.contains('btn-vote')) return;
    const vote = t.getAttribute('data-vote'); // 'right' | 'wrong'
    const jobId = t.getAttribute('data-id') || '';
    const title = t.getAttribute('data-title') || '';
    const url = t.getAttribute('data-url') || '';
    t.disabled = true;
    try{
      const payload = {
        jobId, title, url,
        flag: vote === 'wrong' ? 'not general vacancy' : 'right'
      };
      await fetch(feedbackEndpoint, {
        method:'POST',
        headers:{'Content-Type':'application/json'},
        body: JSON.stringify({ type: 'report', payload })
      });
      t.textContent = vote === 'wrong' ? 'Reported ✖' : 'Thanks ✔';
    }catch(_){
      t.disabled = false;
    }
  });

  // Show missing vacancy form
  const showMissingBtn = document.getElementById('btn-show-missing-form');
  if (showMissingBtn) {
    showMissingBtn.addEventListener('click', () => {
      document.getElementById('missing-modal').style.display = 'block';
    });
  }

  const reportForm = document.getElementById('reportForm');
  if (reportForm) reportForm.addEventListener('submit', handleFormSubmit);

  const missingForm = document.getElementById('missingForm');
  if (missingForm) missingForm.addEventListener('submit', handleFormSubmit);

  document.querySelectorAll('.modal button[type="button"]').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.target.closest('.modal').style.display = 'none';
    });
  });
}

async function handleFormSubmit(e) {
  e.preventDefault();
  const form = e.target;
  const formType = form.id === 'reportForm' ? 'report' : 'missing';
  let payload;
  if (formType === 'report') {
    payload = {
      listingId: document.getElementById('reportListingId').value,
      reason: document.getElementById('reportReason').value,
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
    if (response.ok) {
      alert('Thank you, your submission was received!');
      form.closest('.modal').style.display = 'none';
      form.reset();
    } else {
      alert('Failed to submit. Please try again.');
    }
  } catch (error) {
    alert('An error occurred. Please try again.');
  }
}

function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function escapeAttr(s){ return String(s||'').replace(/"/g,'&quot;'); }

document.addEventListener('DOMContentLoaded', () => {
  renderStatusBanner();
  renderJobs();
  setupFeedbackForms();
});
