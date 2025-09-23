// app.js v2025-09-23-4 — modular equivalent of inline logic with cache-busting and resilient saves

(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";
  const bust = () => "?t=" + Date.now();

  const qs = (s, r)=> (r||document).querySelector(s);
  const qsa = (s, r)=> Array.from((r||document).querySelectorAll(s));
  const esc = (s)=> (s==null? "": String(s)).replace(/[&<>\"']/g, c=>({"&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":""}[c]));
  const fmtDate = (s)=> s && s.toUpperCase()!=="N/A" ? s : "N/A";
  const toast = (msg)=>{ const t=qs("#toast"); if(!t) return alert(msg); t.textContent=msg; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>{ t.style.opacity="0"; },1600); };

  // Tabs
  function activate(tab){
    qsa(".tab").forEach(t=>t.classList.toggle("active", t.dataset.tab===tab));
    qsa(".panel").forEach(p=>p.classList.toggle("active", p.id==="panel-"+tab));
  }
  document.addEventListener("click", (e)=>{ const t=e.target.closest(".tab"); if(t) activate(t.dataset.tab); });

  // State
  let USER_STATE={}, USER_VOTES={};
  async function loadUserStateServer(){ try{ const r=await fetch("user_state.json"+bust(),{cache:"no-store"}); if(r.ok){ USER_STATE=await r.json(); } }catch{} }
  function loadUserStateLocal(){ try{ const j=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(j) USER_STATE=Object.assign({}, USER_STATE, j); }catch{} }
  function setUserStateLocal(id, action){ if(!id) return; if(action==="undo") delete USER_STATE[id]; else USER_STATE[id]={action, ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_state", JSON.stringify(USER_STATE)); }catch{} }
  function loadVotesLocal(){ try{ USER_VOTES=JSON.parse(localStorage.getItem("vac_user_votes")||"{}")||{}; }catch{ USER_VOTES={}; } }
  function setVoteLocal(id, vote){ USER_VOTES[id]={vote, ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_votes", JSON.stringify(USER_VOTES)); }catch{} }
  function clearVoteLocal(id){ delete USER_VOTES[id]; try{ localStorage.setItem("vac_user_votes", JSON.stringify(USER_VOTES)); }catch{} }

  // Outbox
  let OUTBOX = (function(){ try{ return JSON.parse(localStorage.getItem("vac_outbox")||"[]"); }catch{ return []; } })();
  function saveOutbox(){ try{ localStorage.setItem("vac_outbox", JSON.stringify(OUTBOX)); }catch{} }
  async function flushOutbox(){
    if(!OUTBOX.length) return;
    let rest=[];
    for(const p of OUTBOX){
      try{ const r=await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(p)}); if(!r.ok) rest.push(p); }
      catch{ rest.push(p); }
    }
    OUTBOX=rest; saveOutbox();
  }
  async function postJSON(payload){
    try{
      const r=await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)});
      if(!r.ok) throw new Error("net");
    }catch{
      OUTBOX.push(payload); saveOutbox(); setTimeout(flushOutbox, 1500);
    }
    return true;
  }

  async function renderStatus(){
    const pill=qs("#health-pill"), last=qs("#last-updated"), total=qs("#total-listings");
    try{
      const h=await (await fetch("health.json"+bust(),{cache:"no-store"})).json();
      pill.textContent=h.ok?"Health: OK":"Health: Not OK"; pill.className="pill "+(h.ok?"ok":"bad");
      const ts=h.lastUpdated||""; last.textContent="Last updated: "+(ts?new Date(ts).toLocaleString():"—");
      total.textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      pill.textContent="Health: Unknown"; pill.className="pill";
      last.textContent="Last updated: —"; total.textContent="Listings: —";
    }
  }

  function setLocked(btn, label){ btn.textContent = label; btn.classList.add("disabled"); btn.setAttribute("aria-disabled","true"); }
  function addUndo(container, onUndo, seconds){
    const u=document.createElement("button");
    u.className="btn ghost tiny"; u.textContent="Undo ("+seconds+"s)";
    container.appendChild(u);
    let left=seconds; const iv=setInterval(()=>{ left--; if(left<=0){ clearInterval(iv); u.remove(); } else { u.textContent="Undo ("+left+"s)"; } },1000);
    u.addEventListener("click", (e)=>{ e.preventDefault(); e.stopPropagation(); clearInterval(iv); u.remove(); onUndo(); });
  }

  function cardHTML(j, applied=false){
    const trusted=j.flags&&j.flags.trusted, chip=trusted?'<span class="chip ok">✓ trusted</span>':"";
    const d=j.daysLeft!=null?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const appliedBadge = applied ? '<span class="badge-done">Applied</span>' : '';
    const srcRibbon = (j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const lid=j.id||"", vote = USER_VOTES[lid]?.vote || "";
    const voteTick = vote==="right" ? '<span class="chip tick" title="Verified by user">✓</span>' : "";
    const verifiedClass = vote==="right" ? " verified" : "";

    return '<article class="card'+(applied?' applied':'')+verifiedClass+'" data-id="'+esc(lid)+'">'+
      '<header class="card-head"><h3 class="title">'+esc(j.title||"No Title")+'</h3>'+srcRibbon+voteTick+chip+appliedBadge+'</header>'+
      '<div class="card-body">'+
        '<div class="rowline"><span class="muted">Organization</span><span>'+esc(j.organization||"N/A")+'</span></div>'+
        '<div class="rowline"><span class="muted">Qualification</span><span>'+esc(j.qualificationLevel||"N/A")+'</span></div>'+
        '<div class="rowline"><span class="muted">Domicile</span><span>'+esc(j.domicile||"All India")+'</span></div>'+
        '<div class="rowline"><span class="muted">Last date</span><span>'+esc(fmtDate(j.deadline))+' <span class="muted">('+d+' days)</span></span></div>'+
      '</div>'+
      '<footer class="card-actions">'+
        '<span class="hide-on-verified">'+
          '<button class="btn danger" data-act="report">Report</button>'+
          '<button class="btn ok" data-act="right">Right</button>'+
          '<button class="btn warn" data-act="wrong">Wrong</button>'+
        '</span>'+
        '<a class="btn primary" href="'+det+'" target="_blank" rel="noopener">Details</a>'+
        '<span class="spacer"></span>'+
        (applied ? '<button class="btn" data-act="exam_done">Exam done</button>'
                 : '<button class="btn" data-act="applied">Applied</button><button class="btn" data-act="not_interested">Not interested</button>')+
      '</footer>'+
    '</article>';
  }

  function sortByDeadline(list){
    const parse = (s)=>{ if(!s||s.toUpperCase()==="N/A") return null; const t=s.replaceAll("-","/"); const a=t.split("/"); if(a.length!==3) return null; const ms=Date.UTC(+a[2], +a[1]-1, +a[0]); return isNaN(ms)?null:ms; };
    return list.slice().sort((a,b)=>{ const da=parse(a.deadline), db=parse(b.deadline); if(da===null && db===null) return (a.title||"").localeCompare(b.title||""); if(da===null) return 1; if(db===null) return -1; return da - db; });
  }

  let renderToken = 0;
  async function render(){
    const myToken = ++renderToken;

    await loadUserStateServer(); loadUserStateLocal(); loadVotesLocal(); await flushOutbox();

    const dataResp = await fetch("data.json"+bust(),{cache:"no-store"});
    const data = await dataResp.json();
    if (myToken !== renderToken) return;

    const rootOpen=qs("#open-root"), rootApp=qs("#applied-root"), rootOther=qs("#other-root");
    const list = sortByDeadline(Array.isArray(data.jobListings)? data.jobListings : []);
    const sections = data.sections || {};
    qs("#total-listings").textContent = "Listings: " + list.length;

    const idsApplied = new Set(sections.applied||[]);
    const idsOther = new Set(sections.other||[]);
    Object.entries(USER_STATE).forEach(([jid, s])=>{
      if(!s || !s.action) return;
      if(s.action==="applied"){ idsApplied.add(jid); idsOther.delete(jid); }
      if(s.action==="not_interested"){ idsOther.add(jid); idsApplied.delete(jid); }
      if(s.action==="undo"){ idsApplied.delete(jid); idsOther.delete(jid); }
    });

    const fOpen=document.createDocumentFragment(), fApp=document.createDocumentFragment(), fOther=document.createDocumentFragment();
    if(list.length===0){ rootOpen.innerHTML='<div class="empty">No active job listings found.</div>'; return; }

    for(const job of list){
      const isApplied = idsApplied.has(job.id);
      const wrap=document.createElement("div"); wrap.innerHTML = cardHTML(job, isApplied);
      const card=wrap.firstElementChild;

      card.addEventListener("click", async (e)=>{
        const b=e.target.closest("[data-act]"); if(!b) return;
        if(b.classList.contains("disabled")) return;
        const act=b.getAttribute("data-act"), id=card.getAttribute("data-id"),
              title=(card.querySelector(".title")?.textContent||"").trim().slice(0,240),
              detailsUrl=(card.querySelector("a.primary")?.href||"");
        const actions = card.querySelector(".hide-on-verified");

        if(act==="report"){
          qs("#reportListingId").value=id; const m=qs("#report-modal"); m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); return;
        }
        if(act==="right"){
          setVoteLocal(id, "right"); card.classList.add("verified"); if(actions) actions.style.display="none";
          const undoFn = async ()=>{ clearVoteLocal(id); await postJSON({ type:"vote", vote:"undo_right", jobId:id, title, url:detailsUrl, ts:new Date().toISOString() }); await render(); };
          addUndo(card.querySelector(".card-actions"), undoFn, 10);
          postJSON({ type:"vote", vote:"right", jobId:id, title, url:detailsUrl, ts:new Date().toISOString() });
          return;
        }
        if(act==="wrong"){
          setVoteLocal(id, "wrong"); setLocked(b,"Marked ✖");
          const undoFn = async ()=>{ clearVoteLocal(id); await postJSON({ type:"vote", vote:"undo_wrong", jobId:id, title, url:detailsUrl, ts:new Date().toISOString() }); await render(); };
          addUndo(card.querySelector(".card-actions"), undoFn, 10);
          postJSON({ type:"vote", vote:"wrong", jobId:id, title, url:detailsUrl, ts:new Date().toISOString() });
          return;
        }
        if(act==="applied"||act==="not_interested"){
          setUserStateLocal(id, act);
          const undoFn = async ()=>{ setUserStateLocal(id,"undo"); await postJSON({ type:"state", payload:{ jobId:id, action:"undo", ts:new Date().toISOString() } }); await render(); };
          addUndo(card.querySelector(".card-actions"), undoFn, 10);
          postJSON({ type:"state", payload:{ jobId:id, action:act, ts:new Date().toISOString() } });
          await render(); return;
        }
        if(act==="exam_done"){ setLocked(b,"Done ✓"); return; }
      });

      if(isApplied) fApp.appendChild(card);
      else if(idsOther.has(job.id)) fOther.appendChild(card);
      else fOpen.appendChild(card);
    }

    rootOpen.replaceChildren(fOpen);
    rootApp.replaceChildren(fApp);
    rootOther.replaceChildren(fOther);
    scheduleDeadlineAlerts(list);
  }

  function openModal(id){ const m=qs(id); if(m){ m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); } }
  function closeModal(el){ const m=el.closest(".modal"); if(m){ m.classList.add("hidden"); m.setAttribute("aria-hidden","true"); } }
  document.addEventListener("click", (e)=>{ if(e.target && e.target.hasAttribute("data-close")) closeModal(e.target); });

  document.addEventListener("DOMContentLoaded", async ()=>{
    await renderStatus();
    await render();

    qs("#btn-missing")?.addEventListener("click", ()=>openModal("#missing-modal"));

    qs("#reportForm")?.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const id=qs("#reportListingId").value.trim();
      const rc = qs("#reportReason").value || "";
      const ev = qs("#reportEvidenceUrl").value.trim();
      const posts = qs("#reportPosts").value.trim();
      const note = qs("#reportNote").value.trim();
      if(!id || !rc) return toast("Please select a reason.");
      await postJSON({ type:"report", jobId:id, reasonCode:rc, evidenceUrl:ev, posts:posts||null, note, ts:new Date().toISOString() });
      closeModal(e.target); e.target.reset(); toast("Reported.");
    });

    const SUBMIT_LOCK=new Set();
    qs("#missingForm")?.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const title=qs("#missingTitle").value.trim();
      const url=qs("#missingUrl").value.trim();
      const site=qs("#missingSite").value.trim();
      const last=qs("#missingLastDate").value.trim();
      const posts=qs("#missingPosts").value.trim();
      const note=qs("#missingNote").value.trim();
      if(!title||!url) return toast("Post name and notification link are required.");
      const key=url.replace(/[?#].*$/,"").toLowerCase();
      if(SUBMIT_LOCK.has(key)) return toast("Already submitted.");
      SUBMIT_LOCK.add(key);
      await postJSON({ type:"missing", title, url, officialSite:site, lastDate:last, posts:posts||null, note, ts:new Date().toISOString() });
      closeModal(e.target); e.target.reset(); toast("Saved. Will appear after refresh.");
    });
  });

  async function scheduleDeadlineAlerts(list){
    try{ if(Notification && Notification.permission==="default"){ await Notification.requestPermission(); } }catch(e){}
    const notified = JSON.parse(localStorage.getItem("vac_deadline_notified")||"{}");
    const today = new Date(); today.setHours(0,0,0,0);
    function parseDDMMYYYY(s){ if(!s||s.toUpperCase()==="N/A") return null; const a=s.replaceAll("-","/").split("/"); if(a.length!==3) return null; const d=new Date(+a[2], +a[1]-1, +a[0]); d.setHours(0,0,0,0); return isNaN(d.getTime())?null:d; }
    for(const j of list){
      const d=parseDDMMYYYY(j.deadline); if(!d) continue;
      const diffDays = Math.round((d.getTime()-today.getTime())/86400000);
      if(diffDays<=2 && diffDays>=0){
        const id=j.id||esc(j.title||""); if(notified[id]) continue;
        toast((j.title||"") + " ends in "+diffDays+" day(s).");
        notified[id]=true;
      }
    }
    localStorage.setItem("vac_deadline_notified", JSON.stringify(notified));
  }
})();
