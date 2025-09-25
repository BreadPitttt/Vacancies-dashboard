// app.js v2025-09-25-fixes — modal open reliability, posts on cards, 10s Undo with confirm for state changes, layout guard
(function(){
  const ENDPOINT = "https://vacancy.animeshkumar97.workers.dev";

  const qs=(s,r)=>(r||document).querySelector(s);
  const qsa=(s,r)=>Array.from((r||document).querySelectorAll(s));
  const esc=(s)=>(s==null?"":String(s)).replace(/[&<>\"']/g,c=>({"&":"&amp;","<":"&lt;","&gt;":"&gt;","\"":"&quot;","'":""}[c]));
  const fmtDate=(s)=>s && s.toUpperCase()!=="N/A" ? s.replaceAll("-", "/") : "N/A";
  const toast=(m)=>{const t=qs("#toast"); if(!t) return alert(m); t.textContent=m; t.style.opacity="1"; clearTimeout(t._h); t._h=setTimeout(()=>t.style.opacity="0",1800); };
  const bust=(p)=>p+(p.includes("?")?"&":"?")+"t="+Date.now();

  // Tabs
  document.addEventListener("click",(e)=>{ const t=e.target.closest(".tab"); if(!t) return;
    qsa(".tab").forEach(x=>x.classList.toggle("active",x===t));
    qsa(".panel").forEach(p=>p.classList.toggle("active", p.id==="panel-"+t.dataset.tab));
  });

  // State stores
  let USER_STATE={}, USER_VOTES={};
  async function loadUserStateServer(){ try{ const r=await fetch(bust("user_state.json"),{cache:"no-store"}); if(r.ok) USER_STATE=await r.json(); }catch{} }
  function loadUserStateLocal(){ try{ const j=JSON.parse(localStorage.getItem("vac_user_state")||"{}"); if(j) USER_STATE=Object.assign({},USER_STATE,j);}catch{} }
  function setUserStateLocal(id,a){ if(!id) return; if(a==="undo") delete USER_STATE[id]; else USER_STATE[id]={action:a,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_state",JSON.stringify(USER_STATE)); }catch{} }
  function loadVotesLocal(){ try{ USER_VOTES=JSON.parse(localStorage.getItem("vac_user_votes")||"{}")||{}; }catch{ USER_VOTES={}; } }
  function setVoteLocal(id,v){ USER_VOTES[id]={vote:v,ts:new Date().toISOString()}; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }
  function clearVoteLocal(id){ delete USER_VOTES[id]; try{ localStorage.setItem("vac_user_votes",JSON.stringify(USER_VOTES)); }catch{} }

  // Outbox
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
      qs("#last-updated").textContent="Last updated: "+(h.lastUpdated? new Date(h.lastUpdated).toLocaleString() : "—");
      qs("#total-listings").textContent="Listings: "+(typeof h.totalListings==="number"?h.totalListings:"—");
    }catch{
      qs("#health-pill").textContent="Health: Unknown";
      qs("#health-pill").className="pill";
      qs("#last-updated").textContent="Last updated: —";
      qs("#total-listings").textContent="Listings: —";
    }
  }

  const trustedChip=()=>' <span class="chip trusted">trusted</span>';

  // Inline 10s Undo helper (reused across actions)
  function addInlineUndo(container,onUndo,seconds=10){
    if(!container) return;
    const u=document.createElement("button");
    u.className="btn ghost tiny"; let left=seconds; u.textContent=`Undo (${left}s)`;
    container.appendChild(u);
    const iv=setInterval(()=>{ left--; if(left<=0){ clearInterval(iv); u.remove(); } else { u.textContent=`Undo (${left}s)`; } },1000);
    u.addEventListener("click",(e)=>{ e.preventDefault(); e.stopPropagation(); clearInterval(iv); u.remove(); onUndo(); });
  }

  // Global confirm dialog for state changes (not for votes)
  function confirmAction(message="Proceed?"){
    return new Promise((resolve)=>{
      const box=qs("#confirm");
      qs("#confirm-text").textContent=message;
      box.style.display="flex";
      const ok=qs("#confirm-ok"), cancel=qs("#confirm-cancel");
      const cleanup=()=>{ box.style.display="none"; ok.onclick=null; cancel.onclick=null; };
      ok.onclick=()=>{ cleanup(); resolve(true); };
      cancel.onclick=()=>{ cleanup(); resolve(false); };
    });
  }

  // Card template: Organization removed; Posts added (uses numberOfPosts | flags.posts | N/A)
  function cardHTML(j, applied=false){
    const src=(j.source||"").toLowerCase()==="official" ? '<span class="chip" title="Official source">Official</span>' : '<span class="chip" title="From aggregator">Agg</span>';
    const d=(j.daysLeft!=null && j.daysLeft!=="")?j.daysLeft:"—";
    const det=esc(j.detailLink||j.applyLink||"#");
    const lid=j.id||"";
    const vote=USER_VOTES[lid]?.vote||"";
    const tick= vote==="right" ? '<span class="chip ok" title="Verified by user">✓</span>' : "";
    const verified= vote==="right" ? " verified" : "";
    const trust = j.flags && j.flags.trusted ? trustedChip() : "";
    const appliedBadge = applied ? '<span class="badge-done">Applied</span>' : "";
    const posts = (j.numberOfPosts!=null && j.numberOfPosts!=="")
                  ? String(j.numberOfPosts)
                  : (j.flags && j.flags.posts ? String(j.flags.posts) : "N/A");

    return [
      '<article class="card', (applied?' applied':''), verified, '" data-id="', esc(lid), '">',
        '<header class="card-head"><h3 class="title">', esc(j.title||"No Title"), '</h3>', src, tick, trust, appliedBadge, '</header>',
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
            '<button class="btn ok" data-act="right" type="button">Right</button>',
            '<button class="btn warn" data-act="wrong" type="button">Wrong</button>',
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

    await loadUserStateServer(); loadUserStateLocal(); loadVotesLocal(); await flushOutbox();

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

      // Button handler (modal opening fixed)
      card.addEventListener("click", async (e)=>{
        const btn=e.target.closest("[data-act]"); if(!btn) return;
        e.preventDefault(); e.stopPropagation();
        const act=btn.getAttribute("data-act"), id=card.getAttribute("data-id");
        const detailsUrl=(card.querySelector(".row1 .left a")?.href||"");
        const interestCell=card.querySelector(".row2 .interest");

        if(act==="report"){
          const m=qs("#report-modal"); if(!m) return;
          qs("#reportListingId").value=id||"";
          m.classList.remove("hidden"); m.setAttribute("aria-hidden","false");
          return;
        }
        if(act==="right"){ // no confirm for votes
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"right"); card.classList.add("verified");
          addInlineUndo(interestCell, async ()=>{
            if(prev==="right"){ clearVoteLocal(id); } else { setVoteLocal(id,prev||""); }
            await postJSON({type:"vote",vote:"undo_right",jobId:id,url:detailsUrl,ts:new Date().toISOString()});
            await render();
          });
          postJSON({type:"vote",vote:"right",jobId:id,url:detailsUrl,ts:new Date().toISOString()});
          return;
        }
        if(act==="wrong"){ // no confirm for votes
          const prev=USER_VOTES[id]?.vote||"";
          setVoteLocal(id,"wrong"); btn.textContent="Marked ✖"; btn.classList.add("disabled");
          addInlineUndo(interestCell, async ()=>{
            if(prev==="wrong"){ clearVoteLocal(id); } else { setVoteLocal(id,prev||""); }
            await postJSON({type:"vote",vote:"undo_wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()});
            await render();
          });
          postJSON({type:"vote",vote:"wrong",jobId:id,url:detailsUrl,ts:new Date().toISOString()});
          return;
        }
        if(act==="applied"||act==="not_interested"){
          const confirmMsg = act==="applied" ? "Mark as Applied?" : "Move to Other (Not interested)?";
          const ok = await confirmAction(confirmMsg);
          if(!ok) return;
          const prev=USER_STATE[id]?.action||"";
          setUserStateLocal(id,act);
          addInlineUndo(interestCell, async ()=>{
            if(prev){ setUserStateLocal(id,prev); } else { setUserStateLocal(id,"undo"); }
            await postJSON({type:"state",payload:{jobId:id,action:"undo",ts:new Date().toISOString()}});
            await render();
          });
          await postJSON({type:"state",payload:{jobId:id,action:act,ts:new Date().toISOString()}});
          await render(); return;
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

  // Modal open/close (fixes: ensure display toggles correctly and overlay click closes)
  function openModal(sel){ const m=qs(sel); if(m){ m.classList.remove("hidden"); m.setAttribute("aria-hidden","false"); m.style.display="flex"; } }
  function closeModalEl(el){ const m=el.closest(".modal"); if(m){ m.classList.add("hidden"); m.setAttribute("aria-hidden","true"); m.style.display="none"; } }
  document.addEventListener("click",(e)=>{
    if(e.target && e.target.hasAttribute("data-close")){ e.preventDefault(); closeModalEl(e.target); }
    if(e.target && e.target.classList.contains("modal")){ e.preventDefault(); e.target.classList.add("hidden"); e.target.setAttribute("aria-hidden","true"); e.target.style.display="none"; }
  });

  document.addEventListener("DOMContentLoaded", async ()=>{
    await renderStatus(); await render();

    // Submit missing open button
    qs("#btn-missing")?.addEventListener("click",(e)=>{ e.preventDefault(); e.stopPropagation(); openModal("#missing-modal"); });

    // Delegate form submissions
    document.addEventListener("submit", async (e)=>{
      const f=e.target;
      if(f && f.id==="reportForm"){
        e.preventDefault(); e.stopPropagation();
        const id=qs("#reportListingId").value.trim();
        const rc=document.getElementById("reportReason").value||"";
        const ev=document.getElementById("reportEvidenceUrl")?.value?.trim()||"";
        const posts=document.getElementById("reportPosts")?.value?.trim()||"";
        const note=document.getElementById("reportNote")?.value?.trim()||"";
        if(!id||!rc) return toast("Please select a reason.");
        await postJSON({type:"report",jobId:id,reasonCode:rc,evidenceUrl:ev,posts:posts||null,note,ts:new Date().toISOString()});
        closeModalEl(f); f.reset(); toast("Reported.");
      }
      if(f && f.id==="missingForm"){
        e.preventDefault(); e.stopPropagation();
        const title=document.getElementById("missingTitle").value.trim();
        const url=document.getElementById("missingUrl").value.trim();
        const site=document.getElementById("missingSite").value.trim();
        const last=document.getElementById("missingLastDate").value.trim();
        const posts=document.getElementById("missingPosts")?.value?.trim()||"";
        const note=document.getElementById("missingNote").value.trim();
        if(!title||!url) return toast("Post name and notification link are required.");
        const SUBMIT_LOCK=(window.__SUBMIT_LOCK__ ||= new Set());
        const key=url.replace(/[?#].*$/,"").toLowerCase(); if(SUBMIT_LOCK.has(key)) return toast("Already submitted."); SUBMIT_LOCK.add(key);
        await postJSON({type:"missing",title,url,officialSite:site,lastDate:last,posts:posts||null,note,ts:new Date().toISOString()});
        closeModalEl(f); f.reset(); toast("Saved. Will appear after refresh.");
      }
    });
  });
})();
