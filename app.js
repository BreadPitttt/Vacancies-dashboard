// app.js v2025-09-24-stable-align — stable JSON loader + fixed two-row layout + reliable modals
(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";

  const qs=(s,r)=>(r||document).querySelector(s);
  const qsa=(s,r)=>Array.from((r||document).querySelectorAll(s));
  const esc=(s)=>(s==null?"":String(s)).replace(/[&<>\"']/g,c=>({"&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":""}[c]));
  const fmtDate=(s)=>s && s.toUpperCase()!=="N/A" ? s : "N/A";
  const toast=(m)=>{const t=qs("#toast"); if(!t) return alert(m); t.textContent=m; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>t.style.opacity="0",1600); };

  function bust(path){ return path + (path.includes("?")?"&":"?") + "t=" + Date.now(); }

  // Tabs
  document.addEventListener("click",(e)=>{ const t=e.target.closest(".tab"); if(!t) return; qsa(".tab").forEach(x=>x.classList.toggle("active",x===t)); qsa(".panel").forEach(p=>p.classList.toggle("active", p.id==="panel-"+t.dataset.tab)); });

  // Self-learning storage
  let USER_STATE={}, USER_VOTES={};
  async function loadUserStateServer(){ try{ const r=await fetch(bust("user_state.json"),{cache:"no-store"}); if(r.ok) USER_STATE=await r.json(); }catch{} }
  function loadUserStateLocal(){ try{ const j=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(j) USER_STATE=Object.assign({},USER_STATE,j);}catch{} }
  function setUserStateLocal(id,a){ if(!id) return; if(a==="undo") delete USER_STATE[id]; else USER_STATE[id]={action:a,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_state",JSON.stringify(USER_STATE)); }catch{} }
  function loadVotesLocal(){ try{ USER_VOTES=JSON.parse(localStorage.getItem("vac_user_votes")||"{}")||{}; }catch{ USER_VOTES={}; } }
  function setVoteLocal(id,v){ USER_VOTES[id]={vote:v,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }
  function clearVoteLocal(id){ delete USER_VOTES[id]; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }

  // Outbox with retry
  let OUTBOX=(function(){ try{ return JSON.parse(localStorage.getItem("vac_outbox")||"[]"); }catch{ return []; }})();
  function saveOutbox(){ try{ localStorage.setItem("vac_outbox",JSON.stringify(OUTBOX)); }catch{} }
  async function flushOutbox(){ if(!OUTBOX.length) return; let rest=[]; for(const p of OUTBOX){ try{ const r=await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(p)}); if(!r.ok) rest.push(p);}catch{rest.push(p);} } OUTBOX=rest; saveOutbox(); }
  async function postJSON(payload){ try{ const r=await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(payload)}); if(!r.ok) throw new Error("net"); }catch{ OUTBOX.push(payload); saveOutbox(); setTimeout(flushOutbox,1500); } return true; }

  // Status
  async function renderStatus(){
    try{
      const r=await fetch(bust("health.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const h=await r.json();
      qs("#health-pill").textContent=h.ok?"Health: OK":"Health: Not OK";
      qs("#health-pill").className="pill "+(h.ok?"ok":"bad");
      qs("#last-updated").textContent="Last updated: "+(h.lastUpdated? new Date(h.lastUpdated).toLocaleString():"—");
      qs("#total-listings").textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      qs("#health-pill").textContent="Health: Unknown";
      qs("#health-pill").className="pill";
      qs("#last-updated").textContent="Last updated: —";
      qs("#total-listings").textContent="Listings: —";
    }
  }

  const trustedChip=()=>' <span class="chip trusted">trusted</span>';

  function cardHTML(j, applied=false){
    const src=(j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const d=j.daysLeft!=null?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const lid=j.id||"";
    const vote=USER_VOTES[lid]?.vote||"";
    const tick= vote==="right" ? '<span class="chip ok" title="Verified by user">✓</span>' : "";
    const verified= vote==="right" ? " verified" : "";
    const trust = j.flags && j.flags.trusted ? trustedChip() : "";
    const appliedBadge = applied ? '<span class="badge-done">Applied</span>' : "";

    return '<article class="card'+(applied?' applied':'')+verified+'" data-id="'+esc(lid)+'">'+
      '<header class="card-head"><h3 class="title">'+esc(j.title||"No Title")+'</h3>'+src+tick+trust+appliedBadge+'</header>'+
      '<div class="card-body">'+
        '<div class="rowline"><span class="muted">Organization</span><span>'+esc(j.organization||"N/A")+'</span></div>'+
        '<div class="rowline"><span class="muted">Qualification</span><span>'+esc(j.qualificationLevel||"N/A")+'</span></div>'+
        '<div class="rowline"><span class="muted">Domicile</span><span>'+esc(j.domicile||"All India")+'</span></div>'+
        '<div class="rowline"><span class="muted">Last date</span><span>'+esc(fmtDate(j.deadline))+' <span class="muted">('+d+' days)</span></span></div>'+
      </div>'+
      '<div class="actions-row row1">'+
        '<div class="left"><a class="btn primary" href="'+det+'" target="_blank" rel="noopener">Details</a></div>'+
        '<div class="right"><button class="btn danger" data-act="report">Report</button></div>'+
      '</div>'+
      '<div class="actions-row row2">'+
        '<div class="group vote"><button class="btn ok" data-act="right">Right</button><button class="btn warn" data-act="wrong">Wrong</button></div>'+
        '<div class="group interest">'+(applied?'<button class="btn applied" data-act="exam_done">Exam done</button>':'<button class="btn applied" data-act="applied">Applied</button><button class="btn other" data-act="not_interested">Not interested</button>')+'</div>'+
      '</div>'+
    '</article>';
  }

  function sortByDeadline(list){ const parse=(s)=>{ if(!s||s.toUpperCase()==="N/A") return null; const a=s.replaceAll("-","/").split("/"); if(a.length!==3) return null; const ms=Date.UTC(+a[2],+a[1]-1,+a[0]); return isNaN(ms)?null:ms; }; return list.slice().sort((a,b)=>{ const da=parse(a.deadline),db=parse(b.deadline); if(da===null&&db===null) return (a.title||"").localeCompare(b.title||""); if(da===null) return 1; if(db===null) return -1; return da-db; }); }

  let TOKEN=0;
  async function render(){
    const my=++TOKEN;
    await loadUserStateServer(); loadUserStateLocal(); loadVotesLocal(); await flushOutbox();

    // Stable data loader from known-good build
    let data=null;
    try{
      const r=await fetch(bust("data.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      data=await r.json();
    }catch{
      data=null;
    }
    if(my!==TOKEN) return;

    const rootOpen=qs("#open-root"), rootApp=qs("#applied-root"), rootOther=qs("#other-root");

    if(!data || !Array.isArray(data.jobListings)){
      rootOpen.innerHTML='<div class="empty">No active job listings found (data.json missing or invalid).</div>';
      return;
    }

    const list=sortByDeadline(data.jobListings||[]);
    const sections=data.sections||{};
    qs("#total-listings").textContent="Listings: "+list.length;

    const idsApplied=new Set(sections.applied||[]), idsOther=new Set(sections.other||[]);
    Object.entries(USER_STATE).forEach(([jid,s])=>{
      if(!s||!s.action)return;
      if(s.action==="applied"){ idsApplied.add(jid); idsOther.delete(jid); }
      if(s.action==="not_interested"){ idsOther.add(jid); idsApplied.delete(jid); }
      if(s.action==="undo"){ idsApplied.delete(jid); idsOther.delete(jid); }
    });

    const fOpen=document.createDocumentFragment(), fApp=document.createDocumentFragment(), fOther=document.createDocumentFragment();
    for(const job of list){
      const applied=idsApplied.has(job.id);
      const wrap=document.createElement("div"); wrap.innerHTML=cardHTML(job,applied);
      const card=wrap.firstElementChild;

      card.addEventListener("click", async (e)=>{
        const btn=e.target.closest("[data-act]"); if(!btn) return; if(btn.classList.contains("disabled")) return;
        const act=btn.getAttribute("data-act"), id=card.getAttribute("data-id");
        const title=(card.querySelector(".title")?.textContent||"").trim().slice(0,240);
        const detailsUrl=(card.querySelector(".row1 .left a")?.href||"");

        if(act==="report"){
          qs("#reportListingId").value=id; const m=qs("#report-modal"); m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); return;
        }
        if(act==="right"){
          setVoteLocal(id,"right"); card.classList.add("verified");
          addUndo(card.querySelector(".row2 .interest"), async ()=>{ clearVoteLocal(id); await postJSON({type:"vote",vote:"undo_right",jobId:id,title,url:detailsUrl,ts:new Date().toISOString()}); await render(); });
          postJSON({type:"vote",vote:"right",jobId:id,title,url:detailsUrl,ts:new Date().toISOString()}); return;
        }
        if(act==="wrong"){
          setVoteLocal(id,"wrong"); btn.textContent="Marked ✖"; btn.classList.add("disabled");
          addUndo(card.querySelector(".row2 .interest"), async ()=>{ clearVoteLocal(id); await postJSON({type:"vote",vote:"undo_wrong",jobId:id,title,url:detailsUrl,ts:new Date().toISOString()}); await render(); });
          postJSON({type:"vote",vote:"wrong",jobId:id,title,url:detailsUrl,ts:new Date().toISOString()}); return;
        }
        if(act==="applied"||act==="not_interested"){
          setUserStateLocal(id,act);
          addUndo(card.querySelector(".row2 .interest"), async ()=>{ setUserStateLocal(id,"undo"); await postJSON({type:"state",payload:{jobId:id,action:"undo",ts:new Date().toISOString()}}); await render(); });
          postJSON({type:"state",payload:{jobId:id,action:act,ts:new Date().toISOString()}}); await render(); return;
        }
        if(act==="exam_done"){ btn.textContent="Done ✓"; btn.classList.add("disabled"); return; }
      });

      if(applied) fApp.appendChild(card);
      else if(idsOther.has(job.id)) fOther.appendChild(card);
      else fOpen.appendChild(card);
    }

    rootOpen.replaceChildren(fOpen);
    rootApp.replaceChildren(fApp);
    rootOther.replaceChildren(fOther);
  }

  function addUndo(container,onUndo){
    const u=document.createElement("button");
    u.className="btn ghost tiny"; let left=10; u.textContent=`Undo (${left}s)`;
    container.appendChild(u);
    const iv=setInterval(()=>{ left--; if(left<=0){ clearInterval(iv); u.remove(); } else { u.textContent=`Undo (${left}s)`; } },1000);
    u.addEventListener("click",(e)=>{ e.preventDefault(); clearInterval(iv); u.remove(); onUndo(); });
  }

  function openModal(id){ const m=qs(id); if(m){ m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); } }
  function closeModal(el){ const m=el.closest(".modal"); if(m){ m.classList.add("hidden"); m.setAttribute("aria-hidden","true"); } }
  document.addEventListener("click",(e)=>{ if(e.target && e.target.hasAttribute("data-close")) closeModal(e.target); });

  document.addEventListener("DOMContentLoaded", async ()=>{
    qs("#main-css")?.addEventListener("error",()=>toast("Stylesheet failed to load."));
    await renderStatus(); await render();

    qs("#btn-missing")?.addEventListener("click",()=>openModal("#missing-modal"));

    // Modals submit reliably
    qs("#reportForm")?.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const id=qs("#reportListingId").value.trim();
      const rc=document.getElementById("reportReason").value||"";
      const ev=document.getElementById("reportEvidenceUrl")?.value?.trim()||"";
      const posts=document.getElementById("reportPosts")?.value?.trim()||"";
      const note=document.getElementById("reportNote")?.value?.trim()||"";
      if(!id||!rc) return toast("Please select a reason.");
      await postJSON({type:"report",jobId:id,reasonCode:rc,evidenceUrl:ev,posts:posts||null,note,ts:new Date().toISOString()});
      closeModal(e.target); e.target.reset(); toast("Reported.");
    });

    const SUBMIT_LOCK=new Set();
    qs("#missingForm")?.addEventListener("submit", async (e)=>{
      e.preventDefault();
      const title=document.getElementById("missingTitle").value.trim();
      const url=document.getElementById("missingUrl").value.trim();
      const site=document.getElementById("missingSite").value.trim();
      const last=document.getElementById("missingLastDate").value.trim();
      const posts=document.getElementById("missingPosts")?.value?.trim()||"";
      const note=document.getElementById("missingNote").value.trim();
      if(!title||!url) return toast("Post name and notification link are required.");
      const key=url.replace(/[?#].*$/,"").toLowerCase(); if(SUBMIT_LOCK.has(key)) return toast("Already submitted.");
      SUBMIT_LOCK.add(key);
      await postJSON({type:"missing",title,url,officialSite:site,lastDate:last,posts:posts||null,note,ts:new Date().toISOString()});
      closeModal(e.target); e.target.reset(); toast("Saved. Will appear after refresh.");
    });
  });
})();
