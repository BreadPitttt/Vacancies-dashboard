// app.js — hardened for timeouts, graceful errors, archive display, and one retry

const state = {
  data: { jobListings: [], transparencyInfo: {} },
  filters: { urgency: new Set(), qual: new Set(), skill: new Set(), state: new Set(), source: new Set() }
};

const $ = (s, r=document)=>r.querySelector(s);
const $$ = (s, r=document)=>Array.from(r.querySelectorAll(s));

function parseISO(d){ return d ? new Date(d) : null; }
function daysLeft(dl){ const d=parseISO(dl); if(!d||Number.isNaN(d)) return null; const ms=d.setHours(23,59,59,999)-Date.now(); return Math.ceil(ms/86400000); }
function isExpired(dl){ const d=parseISO(dl); return d && !Number.isNaN(d) && d.setHours(23,59,59,999) < Date.now(); }

function urgencyChip(deadlineStr){
  const dl=daysLeft(deadlineStr);
  if(dl===null) return {cls:"chip",label:"Open"};
  if(dl<0) return {cls:"chip chip--urgent",label:"Closed"};
  if(dl<=7) return {cls:"chip chip--urgent",label:`Urgent · ${dl}d`};
  if(dl<=15) return {cls:"chip chip--warn",label:`Soon · ${dl}d`};
  return {cls:"chip chip--ok",label:`Open · ${dl}d`};
}
function badgeForSource(src){ const s=(src||"").toLowerCase(); if(s==="official") return "badge badge--official"; if(s==="aggregator") return "badge badge--aggregator"; return "badge"; }

function matchesFilters(job){
  const F=state.filters, inSet=(set,val)=>set.size===0||set.has(val);
  // Derive urgency from deadline so the "Urgent" pill works without a data field
  const dl = daysLeft(job.deadline);
  const derivedUrgency = (dl !== null && dl >= 0 && dl <= 7) ? "urgent" : "";
  if(!inSet(F.urgency, derivedUrgency)) return false;
  if(!inSet(F.qual, job.qualificationLevel||"")) return false;
  if(F.skill.size>0){ const skills=(job.additionalSkills||[]).map(String); for(const need of F.skill) if(!skills.includes(need)) return false; }
  if(!inSet(F.state, job.domicile||"")) return false;
  if(!inSet(F.source,(job.source||"").toLowerCase())) return false;
  return true;
}

const appliedKey=id=>`applied:${id}`;
const hiddenKey =id=>`hide:${id}`;
function isApplied(id){ try{return localStorage.getItem(appliedKey(id))==="1";}catch{return false;} }
function isHidden(id){ try{return localStorage.getItem(hiddenKey(id))==="1";}catch{return false;} }
function setApplied(id,on){ try{ on?localStorage.setItem(appliedKey(id),"1"):localStorage.removeItem(appliedKey(id)); }catch{} }
function setHidden(id,on){ try{ on?localStorage.setItem(hiddenKey(id),"1"):localStorage.removeItem(hiddenKey(id)); }catch{} }

function escapeHTML(s){ return String(s).replace(/[&<>"']/g,m=>({"&":"&amp;","<":"&lt;",">":"&gt;","\"":"&quot;","'":"&#39;"}[m])); }
function escapeAttr(s){ return escapeHTML(s); }

function ensureArchiveSections(jobsRoot){
  let wrap=$("#archiveWrap");
  if(!wrap){
    wrap=document.createElement("section");
    wrap.id="archiveWrap";
    wrap.innerHTML=`<h2 style="margin-top:2rem">Review Archive</h2><div id="appliedArchive" role="list"></div><div id="hiddenArchive" role="list"></div>`;
    jobsRoot.parentNode.appendChild(wrap);
  }
  return { applied: $("#appliedArchive"), hidden: $("#hiddenArchive") };
}

function cardHTML(j){
  const chip=urgencyChip(j.deadline), applied=isApplied(j.id);
  return `
    <h3>${escapeHTML(j.title||"")}</h3>
    <p>${escapeHTML(j.organization||"")}</p>
    <div class="badges">
      ${j.qualificationLevel?`<span class="badge">${escapeHTML(j.qualificationLevel)}</span>`:""}
      ${j.domicile?`<span class="badge">${escapeHTML(j.domicile)}</span>`:""}
      ${j.source?`<span class="${badgeForSource(j.source)}">${escapeHTML(String(j.source).toUpperCase())}</span>`:""}
    </div>
    <p><span class="${chip.cls}">${chip.label}</span></p>
    <div class="actions">
      ${j.applyLink?`<a href="${escapeAttr(j.applyLink)}" target="_blank" rel="noopener noreferrer" class="btn btn--primary">Apply Online</a>`:""}
      ${j.pdfLink?`<a href="${escapeAttr(j.pdfLink)}" target="_blank" rel="noopener noreferrer" class="btn">View Notification</a>`:""}
      <button class="btn js-applied" data-id="${escapeAttr(j.id)}" aria-pressed="${applied?"true":"false"}">${applied?"Applied":"Mark Applied"}</button>
      <button class="btn js-hide" data-id="${escapeAttr(j.id)}">Not Interested</button>
    </div>
  `;
}

function render(){
  const jobsRoot=$("#jobs"); if(!jobsRoot) return;
  const {applied:appliedArch, hidden:hiddenArch}=ensureArchiveSections(jobsRoot);
  jobsRoot.innerHTML=""; appliedArch.innerHTML=""; hiddenArch.innerHTML="";

  // Build from full set so expiredHidden can include previously hidden listings
  const all=(state.data.jobListings||[]).filter(matchesFilters);
  const activeVisible=all.filter(j=>!isHidden(j.id) && !isExpired(j.deadline));
  const expiredApplied=all.filter(j=>isExpired(j.deadline) && isApplied(j.id));
  const expiredHidden =all.filter(j=>isExpired(j.deadline) && isHidden(j.id));

  for(const j of activeVisible){ const card=document.createElement("article"); card.className="card"; card.setAttribute("role","listitem"); card.innerHTML=cardHTML(j); jobsRoot.appendChild(card); }
  for(const j of expiredApplied){ const card=document.createElement("article"); card.className="card card--archive"; card.setAttribute("role","listitem"); card.innerHTML=cardHTML(j); appliedArch.appendChild(card); }
  for(const j of expiredHidden){ const card=document.createElement("article"); card.className="card card--archive"; card.setAttribute("role","listitem"); card.innerHTML=cardHTML(j); hiddenArch.appendChild(card); }

  const statusEl=$("#status"); if(statusEl){ const last=state.data?.transparencyInfo?.lastUpdated||"—"; statusEl.textContent=`Last updated: ${last} • Showing ${activeVisible.length}`; }
}

function initFilters(){
  document.addEventListener("click",(e)=>{
    const pill=e.target.closest(".pill");
    if(pill){
      const facet=pill.dataset.filter, value=pill.dataset.value;
      if(facet && value){
        pill.classList.toggle("pill--active");
        const on=pill.classList.contains("pill--active"); pill.setAttribute("aria-pressed", on?"true":"false");
        const set=state.filters[facet]; if(set){ on?set.add(value):set.delete(value); render(); }
      }else if(pill.id==="clearFilters"){ for(const k of Object.keys(state.filters)) state.filters[k].clear(); $$(".pill.pill--active").forEach(b=>{b.classList.remove("pill--active"); b.setAttribute("aria-pressed","false");}); render(); }
    }
    const applyBtn=e.target.closest(".js-applied");
    if(applyBtn){ const id=applyBtn.dataset.id; const next=!(applyBtn.getAttribute("aria-pressed")==="true"); setApplied(id,next); applyBtn.setAttribute("aria-pressed", next?"true":"false"); applyBtn.textContent=next?"Applied":"Mark Applied"; render(); return; }
    const hideBtn=e.target.closest(".js-hide");
    if(hideBtn){ const id=hideBtn.dataset.id; setHidden(id,true); render(); return; }
  });
}

async function fetchWithTimeout(url, ms){
  let signal; let controller;
  if (typeof AbortSignal!=="undefined" && AbortSignal.timeout) {
    signal = AbortSignal.timeout(ms);
  } else if (typeof AbortController!=="undefined") {
    controller = new AbortController();
    signal = controller.signal;
    setTimeout(()=>{ try{ controller.abort(); }catch{} }, ms);
  }
  return fetch(url, { cache:"no-store", signal });
}

async function loadData(){
  const s=$("#status");
  try{
    let res = await fetchWithTimeout("data.json", 15000);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    state.data = await res.json();
    if(s) s.textContent="Loaded.";
    render();
  }catch(err){
    // One retry after small backoff for transient issues
    try{
      if(s) s.textContent="Retrying…";
      await new Promise(r=>setTimeout(r, 1200));
      const res2 = await fetchWithTimeout("data.json", 15000);
      if(!res2.ok) throw new Error(`HTTP ${res2.status}`);
      state.data = await res2.json();
      if(s) s.textContent="Loaded.";
      render();
      return;
    }catch(err2){
      if(s) s.textContent="Failed to load data.";
      console.error("data.json load failed", err2);
    }
  }
}

document.addEventListener("DOMContentLoaded", ()=>{ initFilters(); loadData(); });
