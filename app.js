// app.js — Self-learning UI aligned with feedback.js routes
const CF_BASE = 'https://649b07cd.vacancies-dashboard.pages.dev';
const feedbackEndpoint = CF_BASE + '/feedback';
const bust = () => `?t=${Date.now()}`;

function escapeHtml(s){ return (s||'').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'''}[c])); }
function escapeAttr(s){ return String(s||'').replace(/"/g,'&quot;'); }
function fmtDate(s){ return s && s.toUpperCase()!=='N/A' ? s : 'N/A'; }

let GLOBAL_STATE = {}; // { jobId: {action:'applied'|'not_interested', ts}}

async function fetchState(){
  try{
    const res = await fetch('user_state.json' + bust(), { cache:'no-store' });
    if (res.ok) GLOBAL_STATE = await res.json();
  }catch(_){}
}

function h(tag, cls, html){
  const e=document.createElement(tag);
  if(cls) e.className=cls;
  if(html!==undefined) e.innerHTML=html;
  return e;
}
function sectionWrap(title){
  const wrap=h('section','');
  wrap.appendChild(h('h2','',title));
  const cont=h('div','cards-grid'); wrap.appendChild(cont);
  return {wrap, cont};
}

async function send(payload){
  try{
    const res = await fetch(feedbackEndpoint, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(payload) });
    return res.ok;
  }catch{return false;}
}

async function submit(card, kind){
  const id = card.getAttribute('data-id') || '';
  const title = (card.querySelector('h3')?.textContent || '').trim().slice(0,240);
  const url = (card.querySelector('a.primary')?.href || '').trim();
  if(!id) return alert('Missing job id.');

  let payload;
  if(kind==='right' || kind==='wrong'){
    payload = { type:'vote', vote:kind, jobId:id, title, url, clientTs:new Date().toISOString() };
  }else if(kind==='report'){
    payload = { type:'report', jobId:id, title, url, note:'user report', clientTs:new Date().toISOString() };
  }else if(kind==='applied' || kind==='not_interested' || kind==='undo'){
    payload = { type:'state', payload:{ jobId:id, action:kind, ts:new Date().toISOString() } };
  } else {
    return;
  }
  const ok = await send(payload);
  if(!ok) alert('Network issue, please retry.');
}

async function renderStatus(){
  const pill = document.getElementById('health-pill');
  const last = document.getElementById('last-updated');
  const total = document.getElementById('total-listings');
  try {
    const res = await fetch('health.json' + bust(), { cache: 'no-store' });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const h = await res.json();
    const ok = !!h.ok;
    pill.textContent = ok ? 'Health: OK' : 'Health: Not OK';
    pill.className = ok ? 'pill ok' : 'pill bad';
    const ts = h.lastUpdated || h.checkedAt || h.lastChecked || '';
    last.textContent = 'Last updated: ' + (ts ? new Date(ts).toLocaleString() : '—');
    const count = typeof h.totalListings === 'number' ? h.totalListings : '—';
    total.textContent = 'Listings: ' + count;
  } catch {
    pill.textContent = 'Health: Unknown';
    pill.className = 'pill';
    last.textContent = 'Last updated: —';
    total.textContent = 'Listings: —';
  }
}

function buildCard(job){
  const card = h('div','job-card');
  card.setAttribute('data-id', job.id || '');
  const trusted = job.flags && job.flags.trusted;
  const badge = trusted ? ' <span class="badge trusted">✓ trusted</span>' : '';
  const daysLeft = job.daysLeft ?? '—';
  card.innerHTML = `
    <h3>${escapeHtml(job.title || 'No Title')}${badge}</h3>
    <p><strong>Organization:</strong> ${escapeHtml(job.organization || 'N/A')}</p>
    <p><strong>Qualification:</strong> ${escapeHtml(job.qualificationLevel || 'N/A')}</p>
    <p><strong>Domicile:</strong> ${escapeHtml(job.domicile || 'All India')}</p>
    <p><strong>Last date:</strong> ${escapeHtml(fmtDate(job.deadline))} (${daysLeft} days left)</p>
    <p class="links">
      <a class="btn primary" href="${escapeAttr(job.applyLink || '#')}" target="_blank" rel="noopener">Apply Here</a>
      <button class="btn btn-report" data-act="report">Report</button>
      <span class="spacer"></span>
      <button class="btn success" data-act="right">✔ Right</button>
      <button class="btn danger" data-act="wrong">✖ Wrong</button>
      <button class="btn outline" data-act="applied">Applied</button>
      <button class="btn outline" data-act="not_interested">Not interested</button>
    </p>
  `;
  card.addEventListener('click', (e)=>{
    const btn = e.target.closest('[data-act]'); if(!btn) return;
    submit(card, btn.getAttribute('data-act'));
  });
  return card;
}

async function render(){
  const app = document.getElementById('jobs-root'); if(!app) return;
  const res = await fetch('data.json' + bust(), { cache:'no-store' });
  const data = await res.json();
  const list = data.jobListings || [];
  const sections = data.sections || {};
  document.getElementById('total-listings').textContent = 'Listings: ' + list.length;

  const idsApplied = new Set(sections.applied || []);
  const idsOther = new Set(sections.other || []);

  const secPrimary = sectionWrap('Open vacancies');
  const secApplied = sectionWrap('Applied (always visible)');
  const secOther = sectionWrap('Other (past date or not interested)');

  for(const job of list){
    const card = buildCard(job);
    if(idsApplied.has(job.id)) secApplied.cont.appendChild(card);
    else if(idsOther.has(job.id)) secOther.cont.appendChild(card);
    else secPrimary.cont.appendChild(card);
  }
  app.innerHTML = '';
  app.appendChild(secPrimary.wrap);
  app.appendChild(secApplied.wrap);
  app.appendChild(secOther.wrap);
}

document.addEventListener('DOMContentLoaded', async ()=>{
  await fetchState();
  await renderStatus();
  await render();
});
