// app.js v2025-09-26-singleTenant
(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";
  const qs=(s,r)=>(r||document).querySelector(s);
  const qsa=(s,r)=>Array.from((r||document).querySelectorAll(s));
  const esc=(s)=>(s==null?"":String(s)).replace(/[&<>\"']/g,c=>({"&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":""}[c]));
  const fmtDate=(s)=>s && s.toUpperCase()!=="N/A" ? s.replaceAll("-", "/") : "N/A";
  const bust=(p)=>p+(p.includes("?")?"&":"?")+"t="+Date.now();
  const toast=(m)=>{const t=qs("#toast"); if(!t) return alert(m); t.textContent=m; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>t.style.opacity="0",1800); };

  function normHref(u){
    try{ const p=new URL(u.trim()); p.hash=""; p.search=""; let s=p.toString(); if(s.endsWith("/")) s=s.slice(0,-1); return s.toLowerCase(); }
    catch{ return (u||"").trim().toLowerCase().replace(/[?#].*$/,"").replace(/\/$/,""); }
  }

  async function renderStatus(){
    try{
      const r=await fetch(bust("health.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const h=await r.json();
      const pill=qs("#health-pill");
      pill.textContent=h.ok?"Health: OK":"Health: Not OK";
      pill.className="pill "+(h.ok?"ok":"bad");
      qs("#last-updated").textContent="Last updated: "+(h.lastUpdated? new Date(h.lastUpdated).toLocaleString() : "—");
      qs("#total-listings").textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      const pill=qs("#health-pill");
      pill.textContent="Health: Unknown";
      pill.className="pill";
      qs("#last-updated").textContent="Last updated: —";
      qs("#total-listings").textContent="Listings: —";
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
    }catch{}
    try{
      const r=await fetch(bust("user_state.json"),{cache:"no-store"}); if(!r.ok) throw 0;
      const remote=await r.json(); if(remote && typeof remote==="object"){ USER_STATE={...remote}; }
    }catch{ USER_STATE={}; }
  }
  function loadUserStateLocal(){
    try{ const local=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(local && typeof local==="object"){ USER_STATE={...USER_STATE, ...local}; } }catch{}
  }
  function setUserStateLocal(id,a){
    if(!id) return;
    if(a==="undo") delete USER_STATE[id];
    else USER_STATE[id]={action:a,ts:new Date().toISOString()};
    try{ localStorage.setItem("vac_user_state",JSON.stringify(USER_STATE)); }catch{}
  }
  async function persistUserStateServer(){
    try{
      await fetch(ENDPOINT,{
        method:"POST",
        headers:{"Content-Type":"application/json"},
        body:JSON.stringify({ type:"user_state_sync", tenantId:"personal", payload:USER_STATE, ts:new Date().toISOString() })
      });
    }catch{}
  }

  function loadVotesLocal(){ try{ USER_VOTES=JSON.parse(localStorage.getItem("vac_user_votes")||"{}")||{}; }catch{ USER_VOTES={}; } }
  function setVoteLocal(id,v){ USER_VOTES[id]={vote:v,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }
  function clearVoteLocal(id){ delete USER_VOTES[id]; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }

  function confirmAction(message="Proceed?"){ return Promise.resolve(confirm(message)); }

  const trustedChip=()=>' <span class="chip trusted">trusted</span>';
  const topVerify=()=>' <span class="verify-top" title="Verified Right">✓</span>';
  const corroboratedChip=()=>' <span class="chip" title="Multiple sources">x2</span>';

  function renderInlineUndo(slot, label, onUndo, onCommit, seconds=10){
    if(!slot) return;
    const wrap=document.createElement("div");
    wrap.className="group";
    const b=document.createElement("button");
    b.className="btn ghost tiny";
    let left=seconds;
    const tick=setInterval(()=>{ left--; if(left<=0){ clearInterval(tick); b.disabled=true; wrap.remove(); onCommit(); } else { b.textContent=`Undo ${label} (${left}s)`; } },1000);
    b.textContent=`Undo ${label} (${left}s)`;
    b.onclick=(ev)=>{ ev.preventDefault(); clearInterval(tick); wrap.remove(); onUndo(); };
    wrap.appendChild(b);
    slot.replaceChildren(wrap);
  }

  function cardHTML(j, applied=false){
    const src=(j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const d=(j.daysLeft!=null && j.daysLeft!=="")?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const lid=j.id||"";
    const vote=USER_VOTES[lid]?.vote||"";
    const verified= vote==="right";
    const trust = j.flags && j.flags.trusted ? trustedChip() : "";
    const corr = j.flags && j.flags.corroborated ? corroboratedChip() : "";
    const tVerify = verified ? topVerify() : "";
    const appliedBadge = applied ? '<span class="badge-done">Applied</span>' : "";
    const posts = (j.numberOfPosts!=null && j.numberOfPosts!=="")
                  ? String(j.numberOfPosts)
                  : (j.flags && j.flags.posts ? String(j.flags.posts) : "N/A");

    return [
      '<article class="card', (applied?' applied':''), (verified?' verified':''), '" data-id="', esc(lid), '">',
        '<header class="card-head"><h3 class="title">', esc(j.title||"No Title"), '</h3>', src, trust, corr, tVerify, appliedBadge, '</header>',
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
          '<div class="group vote">',
            '<button class="vote-btn right" data-act="right" type="button">☑</button>',
            '<button class="vote-btn wrong" data-act="wrong" type="button">☒</button>',
          '</div>',
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
    const parse=(s)=>{ if(!s||s.toUpperCase()==="N/A") return null;
      const a=s.replaceAll("-","/").split("/"); if(a.length!==3) return null;
      const ms=Date.UTC(+a[2],+a[1]-1,+a[0]); return isNaN(ms)?null:ms; };
    return list.slice().sort((a,b)=>{ const da=parse(a.deadline),db=parse(b.deadline);
      if(da===null&&db===null) return (a.title||"").localeCompare(b.title||"");
      if(da===null) return 1; if(db===null) return -1; return da-db; });
  }

  let TOKEN=0;
  async function render(){
    const my=++TOKEN;

    loadVotesLocal();

    let data=null;
    try{ const r=await fetch(bust("data.json"),{cache:"no-store"}); if(!r.ok) throw 0; data=await r.json(); }catch{ data=null; }
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
        const btn=e.target.closest("[data-act]"); if(!btn) return;
        e.preventDefault(); e.stopPropagation();
        const act=btn.getAttribute("data-act"), id=card.getAttribute("data-id");
        const detailsUrl=(card.querySelector(".row1 .left a")?.href||"");
        const voteCell=card.querySelector(".row2 .vote");
        const interestCell=card.querySelector(".row2 .interest");

        if(act==="report"){
          const m=qs("#report-modal"); if(!m) return;
          const titleText = card.querySelector(".title")?.textContent?.trim() || "";
          qs("#reportListingId").value=id||"";
          qs("#reportListingTitle").value=titleText;
          qs("#reportListingUrl").value=detailsUrl;
          m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex";
          setTimeout(()=>qs("#reportReason")?.focus(),0);
          return;
        }

        if(act==="right"){
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"right"); card.classList.add("verified");
          renderInlineUndo(voteCell, "vote",
            async ()=>{ if(prev==="right"){ clearVoteLocal(id); } else { setVoteLocal(id,prev||""); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"undo_right",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await persistUserStateServer(); await render(); },
            async ()=>{ await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"right",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await persistUserStateServer(); await render(); }, 10);
          return;
        }

        if(act==="wrong"){
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"wrong");
          renderInlineUndo(voteCell, "vote",
            async ()=>{ if(prev==="wrong"){ clearVoteLocal(id); } else { setVoteLocal(id,prev||""); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"undo_wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await persistUserStateServer(); await render(); },
            async ()=>{ await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"vote",vote:"wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()})});
              await persistUserStateServer(); await render(); }, 10);
          return;
        }

        if(act==="applied"||act==="not_interested"){
          const ok = await confirmAction(act==="applied" ? "Mark as Applied?" : "Move to Other (Not interested)?");
          if(!ok) return;
          const prev=USER_STATE[id]?.action||"";
          setUserStateLocal(id,act);
          renderInlineUndo(interestCell, act==="applied"?"applied":"choice",
            async ()=>{ if(prev){ setUserStateLocal(id,prev); } else { setUserStateLocal(id,"undo"); }
              await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"state",payload:{jobId:id,action:"undo",ts:new Date().toISOString()}})});
              await persistUserStateServer(); await render(); },
            async ()=>{ await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({type:"state",payload:{jobId:id,action:act,ts:new Date().toISOString()}})});
              await persistUserStateServer(); await render(); }, 10);
          return;
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

  function openModal(sel){
    const m=qs(sel); if(!m) return;
    m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex";
    if(sel==="#missing-modal"){ setTimeout(()=>qs("#missingTitle")?.focus(),0); }
  }
  function closeModalEl(el){
    const m=el.closest(".modal"); if(m){ m.classList.add("hidden"); m.setAttribute("aria-hidden","true"); m.style.display="none"; }
  }
  document.addEventListener("click",(e)=>{
    if(e.target && (e.target.hasAttribute("data-close") || e.target.classList.contains("close-top"))){ e.preventDefault(); closeModalEl(e.target); }
    if(e.target && e.target.classList.contains("modal")){ e.preventDefault(); e.target.classList.add("hidden"); e.target.setAttribute("aria-hidden","true"); e.target.style.display="none"; }
  });

  document.addEventListener("DOMContentLoaded", async ()=>{
    await loadUserStateServer();
    loadUserStateLocal();
    loadVotesLocal();
    await persistUserStateServer();
    await renderStatus();
    await render();

    const SUBMIT_LOCK=(window.__SUBMIT_LOCK__ ||= new Set());

    document.addEventListener("submit", async (e)=>{
      const f=e.target;
      if(f && f.id==="reportForm"){
        e.preventDefault(); e.stopPropagation();
        const id=qs("#reportListingId").value.trim();
        const title=qs("#reportListingTitle").value.trim();
        const url=qs("#reportListingUrl").value.trim();
        const rc=document.getElementById("reportReason").value||"";
        const ev=document.getElementById("reportEvidenceUrl")?.value?.trim()||"";
        const posts=document.getElementById("reportPosts")?.value?.trim()||"";
        const note=document.getElementById("reportNote")?.value?.trim()||"";
        const last=document.getElementById("reportLastDate")?.value?.trim()||"";
        const elig=document.getElementById("reportEligibility")?.value?.trim()||"";
        if(!id||!rc) return toast("Please choose a reason and try again.");
        await fetch(ENDPOINT,{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify({
          type:"report",jobId:id,title,url,reasonCode:rc,evidenceUrl:ev,posts:posts||null,lastDate:last||"",eligibility:elig||"",note,ts:new Date().toISOString()
        })});
        f.reset(); toast("Reported."); document.querySelector('[data-close]')?.click();
      }
    });
  });
})();
