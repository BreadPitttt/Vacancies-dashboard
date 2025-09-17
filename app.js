// app.js — Complete Final Version
const feedbackEndpoint = 'https://phenomenal-cat-7d2563.netlify.app/.netlify/functions/feedback';

/**
 * Renders the health status banner at the top of the page.
 */
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
    const ts = h.lastUpdated || h.checkedAt || '';
    last.textContent = 'Last updated: ' + (ts ? new Date(ts).toLocaleString() : '—');
    const count = typeof h.totalActive === 'number' ? h.totalActive : '—';
    total.textContent = 'Listings: ' + count;
  } catch (e) {
    pill.textContent = 'Health: Unknown';
    last.textContent = 'Last updated: —';
    total.textContent = 'Listings: —';
  }
}

/**
 * Fetches jobs and renders them into job cards.
 */
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

    // Clear previous listings
    mount.innerHTML = '';

    // Create a card for each job
    data.jobListings.forEach(job => {
      if (job.flags && job.flags.hidden) return; // Skip hidden jobs

      const card = document.createElement('div');
      card.className = 'job-card';
      
      // IMPORTANT: This line creates the "Report" button for each card
      card.innerHTML = `
        <h3>${job.title || 'No Title'}</h3>
        <p><strong>Organization:</strong> ${job.organization || 'N/A'}</p>
        <p><strong>Deadline:</strong> ${job.deadline || 'N/A'} (${job.daysLeft !== null ? job.daysLeft : '?'} days left)</p>
        <a href="${job.applyLink}" target="_blank">Apply Here</a>
        <button class="btn-report" data-id="${job.id}">Report</button>
      `;
      mount.appendChild(card);
    });

  } catch (e) {
    mount.innerHTML = `<p style="color: red;">Error loading job listings: ${e.message}</p>`;
  }
}

/**
 * Sets up the event listeners for the feedback forms.
 */
function setupFeedbackForms() {
    // Show the report form when a report button is clicked
    document.addEventListener('click', function(e) {
      if (e.target && e.target.classList.contains('btn-report')) {
        const listingId = e.target.getAttribute('data-id');
        document.getElementById('reportListingId').value = listingId;
        document.getElementById('report-modal').style.display = 'block';
      }
    });

    // Show the missing vacancy form
    const showMissingBtn = document.getElementById('btn-show-missing-form');
    if (showMissingBtn) {
        showMissingBtn.addEventListener('click', () => {
            document.getElementById('missing-modal').style.display = 'block';
        });
    }

    // Handle report form submission
    const reportForm = document.getElementById('reportForm');
    if (reportForm) {
        reportForm.addEventListener('submit', handleFormSubmit);
    }
    
    // Handle missing vacancy form submission
    const missingForm = document.getElementById('missingForm');
    if (missingForm) {
        missingForm.addEventListener('submit', handleFormSubmit);
    }

    // Attach cancel functionality to modals
    document.querySelectorAll('.modal button[type="button"]').forEach(btn => {
        btn.addEventListener('click', (e) => {
            e.target.closest('.modal').style.display = 'none';
        });
    });
}

/**
 * A reusable function to handle form submissions for both report and missing vacancy.
 */
async function handleFormSubmit(e) {
    e.preventDefault();
    const form = e.target;
    const formType = form.id === 'reportForm' ? 'report' : 'missing';
    let payload;

    if (formType === 'report') {
        payload = {
            listingId: document.getElementById('reportListingId').value,
            reason: document.getElementById('reportReason').value,
            evidenceUrl: document.getElementById('reportEvidenceUrl').value.trim() || null,
            note: document.getElementById('reportNote').value.trim() || null
        };
    } else {
        payload = {
            title: document.getElementById('missingTitle').value.trim(),
            url: document.getElementById('missingUrl').value.trim(),
            note: document.getElementById('missingNote').value.trim() || null
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
            body: JSON.stringify({ type: formType, payload: payload })
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

// Run everything when the page loads
document.addEventListener('DOMContentLoaded', () => {
  renderStatusBanner();
  renderJobs();
  setupFeedbackForms();
});
