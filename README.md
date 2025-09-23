# Vacancies-dashboard

A lightweight, automated dashboard that shows general-competition government job vacancies for "Any Graduate," 12th pass, and 10th pass candidates.

[![Data Pipeline Status](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/data-pipeline.yml/badge.svg)](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/data-pipeline.yml) [![GitHub Pages Status](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/pages/pages-build-deployment/badge.svg?branch=main)](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/pages/pages-build-deployment)

# General Vacancy Dashboard

A static dashboard for tracking government vacancies with fast client-side UX:
- Sticky actions: Applied / Not interested and Right / Wrong persist locally and sync to a lightweight endpoint.  
- Deadline awareness: shows days left and sends local notification reminders.  
- Reports and missing submissions: capture corrections and new postings for the next run.

## Live
GitHub Pages serves the site from the default branch (root or /docs). If Actions are queued, see Troubleshooting.

## Project layout
- index.html — UI and client logic (render guard to prevent white flashes, outbox retry for network).  
- style.css — finalized dark UI/UX with the old layout (Details/Report row, action buttons unchanged).  
- scraper.py — light scrapers; emits `numberOfPosts` when possible.  
- qc_and_learn.py — merges updates, normalizes deadlines, learns trust, and preserves user sections.

## Features
- Tabs: Open / Applied / Other.  
- Cards: title, Official/trusted chips, Organization, Qualification, Domicile, Last date, Details/Report.  
- Actions: Right / Wrong with Undo; Applied / Not interested with Undo; Exam done for applied.  
- Number of posts: shown when present; forms allow providing it to the pipeline.

## How it persists actions
- Client writes immediately to `localStorage`:
  - `vac_user_state` for applied/not_interested  
  - `vac_user_votes` for right/wrong  
- A background outbox POSTs to the endpoint and retries later if offline.  
- On each load, local state overlays server sections to keep selections sticky.

## Build and deploy (GitHub Pages)
1. Commit changes to main.  
2. GitHub Actions runs “Pages build and deployment”.  
3. If it’s stuck on “Waiting for a runner to pick up this job”:
   - Cancel the run and click “Re-run all jobs”.  
   - Push a no‑op commit (e.g., edit README) to retrigger.  
   - Ensure `runs-on: ubuntu-latest` (no custom labels).  
   - Check GitHub Status for Actions/Pages incidents.  
   - Confirm Pages is enabled for the branch in Settings → Pages.  

## Local development
Open index.html in a browser (no build step). For fresh CSS after deploy, the link includes `?v=…` to bypass cache.

## Data pipeline (brief)
- `scraper.py` collects listings (official in light mode prioritized) and writes `data.json`.  
- `qc_and_learn.py`:
  - Merges notice updates and extends deadlines when corrigendums indicate.  
  - Normalizes `numberOfPosts` from titles/inputs.  
  - Applies learned trust/demotion from user votes.  
  - Fills
