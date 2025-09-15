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
function badgeForSource(src){ const s=(src||"").toLowerCase(); if(s==="official") return "badge badge--official"; if
