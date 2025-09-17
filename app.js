// app.js — Banner + Jobs Rendering + Feedback Forms
const feedbackEndpoint = 'https://phenomenal-cat-7d2563.netlify.app/.netlify/functions/feedback';

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

async function renderJobsIfNeeded() {
  const mount = document.getElementById('jobs-root');
  if (!mount) return;
  try {
    const res = await fetch('data.json', { cache: 'no-store' });
    if (!res.ok) return;
    const data = await res.json();
    if (!Array.isArray(data.jobListings)) return;
    // Your existing job rendering logic goes here.
    // For now, this is a placeholder.
  } catch {}
}

function setupFeedbackForms() {
    // Show the report form when a report button is clicked
    document.addEventListener('click', function(e) {
      if (e.target && e.target.classList.contains('btn-report')) {
        const listingId = e.target.getAttribute('data-id');
        document.getElementById('reportListingId').value = listingId;
        document.getElementById('report-modal').style.display = 'block';
      }
    });

    // Show the missing vacancy form when its button is clicked
    const showMissingBtn = document.getElementById('btn-show-missing-form');
    if (showMissingBtn) {
        showMissingBtn.addEventListener('click', function() {
            document.getElementById('missing-modal').style.display = 'block';
        });
    }

    // Handle the report form submission
    const reportForm = document.getElementById('reportForm');
    if (reportForm) {
        reportForm.addEventListener('submit', async (e) => {
          e.preventDefault();
          const payload = {
            listingId: document.getElementById('reportListingId').value,
            reason: document.getElementById('reportReason').value,
            evidenceUrl: document.getElementById('reportEvidenceUrl').value.trim() || null,
            note: document.getElementById('reportNote').value.trim() || null
          };
          
          try {
            const response = await fetch(feedbackEndpoint, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ type: 'report', payload: payload })
            });
            if (response.ok) {
              alert('Thank you for your report!');
              document.getElementById('report-modal').style.display = 'none';
              reportForm.reset();
            } else {
              alert('Failed to submit report.');
            }
          } catch (error) {
            alert('Error submitting report.');
          }
        });
    }
    
    // Attach cancel functionality to report modal
    const reportCancelBtn = document.querySelector('#report-modal button[type="button"]');
    if(reportCancelBtn) {
        reportCancelBtn.addEventListener('click', () => {
            document.getElementById('report-modal').style.display = 'none';
        });
    }

    // Handle the missing vacancy form submission
    const missingForm = document.getElementById('missingForm');
    if (missingForm) {
        missingForm.addEventListener('submit', async (e) => {
          e.preventDefault();
          const payload = {
            title: document.getElementById('missingTitle').value.trim(),
            url: document.getElementById('missingUrl').value.trim(),
            note: document.getElementById('missingNote').value.trim() || null
          };

          if (!payload.title || !payload.url) {
            alert('Title and URL are required.');
            return;
          }

          try {
            const response = await fetch(feedbackEndpoint, {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ type: 'missing', payload: payload })
            });
            if (response.ok) {
              alert('Missing vacancy submitted successfully!');
              document.getElementById('missing-modal').style.display = 'none';
              missingForm.reset();
            } else {
              alert('Failed to submit missing vacancy.');
            }
          } catch (error) {
            alert('Error submitting missing vacancy.');
          }
        });
    }
    
    // Attach cancel functionality to missing vacancy modal
    const missingCancelBtn = document.querySelector('#missing-modal button[type="button"]');
    if(missingCancelBtn) {
        missingCancelBtn.addEventListener('click', () => {
            document.getElementById('missing-modal').style.display = 'none';
        });
    }
}

document.addEventListener('DOMContentLoaded', () => {
  renderStatusBanner();
  renderJobsIfNeeded();
  setupFeedbackForms(); // Add this line to run the new feedback code
});
