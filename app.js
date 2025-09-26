// app.js v2025-09-26-final-fix
(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";
  const qs=(s,r)=>(r||document).querySelector(s);
  const qsa=(s,r)=>Array.from((r||document).querySelectorAll(s));
  const esc=(s)=>(s==null?"":String(s)).replace(/[&<>\"']/g,c=>({"&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":""}[c]));
  const fmtDate=(s)=>s && s.toUpperCase()!=="N/A" ? s.replaceAll("-", "/") : "N/A";
  const bust=(p)=>p+(p.includes("?")?"&":"?")+"t="+Date.now();
  const toast=(m)=>{const t=qs("#toast"); if(!t) return alert(m); t.textContent=m; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>t.style.opacity="0",1800); };

  async function renderStatus(){
    try{
      const r=await fetch(bust("health.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const h=await r.json();
      qs("#health-pill").textContent=h.ok?"Health: OK":"Health: Not OK";
      qs("#health-pill").className="pill "+(h.ok?"ok":"bad");
      qs("#last-updated").textContent="Last updated: "+(h.lastUpdated? new Date(h.lastUpdated).toLocaleString() : "—");
      qs("#total-listings").textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      qs("#health-pill").textContent="Health: Unknown"; qs("#health-pill").className="pill";
      qs("#last-updated").textContent="Last updated: —"; qs("#total-listings").textContent="Listings: —";
    }
  }

  document.addEventListener("click",(e)=>{ const t=e.target.closest(".tab"); if(!t) return;
    qsa(".tab").forEach(x=>x.classList.toggle("active",x===t));
    qsa(".panel").forEach(p=>p.classList.toggle("active", p.id==="panel-"+t.dataset.tab));
  });

  let USER_STATE={}, USER_VOTES={};

  async function loadUserStateServer(){
    try{
      const wr=await fetch(ENDPOINT+"?state=1",{mode:"cors"});
      if(wr.ok){
        const wj=await wr.json();
        if(wj && wj.ok && wj.state && typeof wj.state==="object"){ USER_STATE={...wj.state}; return; }
      }
    }catch(e){ console.error("Failed to load user state from server", e); }
    try{
      const r=await fetch(bust("user_state.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const remote=await r.json(); if(remote && typeof remote==="object"){ USER_STATE={...remote}; }
    }catch{ USER_STATE={}; }
  }
  function loadUserStateLocal(){
    try{ const local=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(local && typeof local==="object"){ USER_STATE={...USER_STATE, ...local}; } }catch{}
  }
  async function persistUserStateServer(){
    try{
      await fetch(ENDPOINT,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ type:"user_state_sync", payload:USER_STATE, ts:new Date().toISOString() })
      });
    }catch(e){ console.error("Failed to persist user state", e); }
  }

  function cardHTML(j, applied=false){
    const src=(j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const d=(j.daysLeft!=null && j.daysLeft!=="")?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const lid=j.id||"";
    const posts = (j.numberOfPosts!=null && j.numberOfPosts!=="") ? String(j.numberOfPosts) : "N/A";

    return [
      '<article class="card', (applied?' applied':''), '" data-id="', esc(lid), '">',
        '<header class="card-head"><h3 class="title">', esc(j.title||"No Title"), '</h3>', src, (applied?'<span class="badge-done">Applied</span>':''),'</header>',
        '<div class="card-body">',
          '<div class="rowline"><span class="muted">Posts</span><span>', esc(posts), '</span></div>',
          '<div class="rowline"><span class="muted">Qualification</span><span>', esc(j.qualificationLevel||"N/A"), '</span></div>',
          '<div class="rowline"><span class="muted">Domicile</span><span>', esc(j.domicile||"All India"), '</span></div>',
          '<div class="rowline"><span class="muted">Last date</span><span>', esc(fmtDate(j.deadline)), ' <span class="muted">(', d, ' days)</span></span></div>',
        '</div>',
        '<div class="actions-row row1">',
          '<div class="left"><a class="btn primary" href="', det, '" target="_blank" rel="noopener">Details</a></div>',
          '<div class="right"><button class="btn danger" data-act="report" type="button">Report</button></div>',
        '</div>',
        '<div class="actions-row row2">',
          '<div class="group interest">',
            applied
              ? '<button class="btn applied" data-act="exam_done" type="button">Exam done</button>'
              : '<button class="btn applied" data-act="applied" type="button">Applied</button><button class="btn other" data-act="not_interested" type="button">Not interested</button>',
          '</div>',
        '</div>',
      '</article>'
    ].join('');
  }

  function sortByDeadline(list){
    const parse=(s)=>{ if(!s||s.toUpperCase()==="N/A") return null; const a=s.replaceAll("-","/").split("/"); if(a.length!==3) return null; const ms=Date.UTC(+a[2],+a[1]-1,+a[0]); return isNaN(ms)?null:ms; };
    return list.slice().sort((a,b)=>{ const da=parse(a.deadline),db=parse(b.deadline); if(da===null&&db===null) return (a.title||"").localeCompare(b.title||""); if(da===null) return 1; if(db===null) return -1; return da-db; });
  }

  async function render(){
    let data=null;
    try{ const r=await fetch(bust("data.json"),{cache:"no-store"}); if(!r.ok) throw 0; data=await r.json(); }catch{ data=null; }
    if(!data || !Array.isArray(data.jobListings)){ qs("#open-root").innerHTML='<div class="empty">No listings found.</div>'; return; }

    const list=sortByDeadline(data.jobListings||[]);
    qs("#total-listings").textContent="Listings: "+list.length;
    const idsApplied=new Set((data.sections||{}).applied||[]), idsOther=new Set((data.sections||{}).other||[]);
    Object.entries(USER_STATE).forEach(([jid,s])=>{
      if(s.action==="applied"){ idsApplied.add(jid); idsOther.delete(jid); }
      if(s.action==="not_interested"){ idsOther.add(jid); idsApplied.delete(jid); }
    });

    const fOpen=document.createDocumentFragment(), fApp=document.createDocumentFragment(), fOther=document.createDocumentFragment();
    for(const job of list){
      const applied=idsApplied.has(job.id);
      const wrap=document.createElement("div"); wrap.innerHTML=cardHTML(job,applied);
      const card=wrap.firstElementChild;
      if(applied) fApp.appendChild(card); else if(idsOther.has(job.id)) fOther.appendChild(card); else fOpen.appendChild(card);
    }
    qs("#open-root").replaceChildren(fOpen); qs("#applied-root").replaceChildren(fApp); qs("#other-root").replaceChildren(fOther);
  }

  document.addEventListener("click", async (e)=>{
    const btn=e.target.closest("[data-act]"); if(!btn) return;
    e.preventDefault(); e.stopPropagation();
    const card=btn.closest(".card"), id=card.getAttribute("data-id"), act=btn.getAttribute("data-act");

    if(act==="report"){
      const m=qs("#report-modal"); if(!m) return;
      qs("#reportListingId").value=id||"";
      qs("#reportListingTitle").value = card.querySelector(".title")?.textContent?.trim() || "";
      qs("#reportListingUrl").value = card.querySelector(".actions-row.row1 .left a")?.href || "";
      m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex";
      setTimeout(()=>qs("#reportReason")?.focus(),0);
      return;
    }
    if(act==="applied"||act==="not_interested"){
      USER_STATE[id]={action:act,ts:new Date().toISOString()};
      await persistUserStateServer(); await render();
    }
  });

  function closeModalEl(el){ const m=el.closest(".modal"); if(m){m.classList.add("hidden");m.setAttribute("aria-hidden","true");m.style.display="none";} }
  document.addEventListener("click",(e)=>{
    if(e.target && (e.target.hasAttribute("data-close") || e.target.classList.contains("close-top") || e.target.classList.contains("modal"))){ e.preventDefault(); closeModalEl(e.target); }
  });

  document.addEventListener("DOMContentLoaded", async ()=>{
    await loadUserStateServer();
    loadUserStateLocal();
    await persistUserStateServer();
    await renderStatus();
    await render();

    qs("#btn-missing")?.addEventListener("click",(e)=>{ e.preventDefault(); qs("#missing-modal")?.classList.remove("hidden"); });

    document.addEventListener("submit", async (e)=>{
      e.preventDefault(); const f=e.target;
      if(f.id === "reportForm"){
        const id=qs("#reportListingId").value.trim(), rc=qs("#reportReason").value;
        if(!id||!rc) return toast("Please select a reason.");
        const res = await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
          type:"report", jobId:id, title:qs("#reportListingTitle").value, url:qs("#reportListingUrl").value,
          reasonCode:rc, evidenceUrl:qs("#reportEvidenceUrl").value.trim(), posts:qs("#reportPosts").value.trim()||null,
          lastDate:qs("#reportLastDate").value.trim(), eligibility:qs("#reportEligibility").value.trim(),
          note:qs("#reportNote").value.trim(), ts:new Date().toISOString()
        })});
        if(!res.ok){ toast(`Report failed: ${res.statusText}`); return; }
        f.reset(); closeModalEl(f); toast("Reported.");
      }
      if(f.id === "missingForm"){
        const title=qs("#missingTitle").value.trim(), url=qs("#missingUrl").value.trim();
        if(!title||!url) return toast("Title and URL required.");
        await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
          type:"missing",title,url,officialSite:qs("#missingSite").value.trim(),lastDate:qs("#missingLastDate").value.trim()||"N/A",
          posts:qs("#missingPosts").value.trim()||null,note:qs("#missingNote").value.trim(),ts:new Date().toISOString()
        })});
        f.reset(); closeModalEl(f); toast("Saved.");
      }
    });
  });
})();
