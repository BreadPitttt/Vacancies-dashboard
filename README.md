# Vacancies-dashboard

A lightweight, automated dashboard that shows general-competition government job vacancies for "Any Graduate," 12th pass, and 10th pass candidates.

[![Data Pipeline Status](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/data-pipeline.yml/badge.svg)](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/data-pipeline.yml) [![GitHub Pages Status](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/pages/pages-build-deployment/badge.svg?branch=main)](https://github.com/BreadPitttt/Vacancies-dashboard/actions/workflows/pages/pages-build-deployment)

## Operations

This project uses an aggregators-first discovery with official-first verification approach:

- Discovery: Primaries (Adda247, sarkariresult.com.cm) for fast detection; backups (SarkariExam, RojgarResult, ResultBharat) for corroboration.
- Verification: Publish only when either an official-domain link is present or at least two distinct aggregators corroborate the same normalized key.
- Strict open-window: Only listings with a valid non-expired deadline are published; expired ones are dropped automatically.

### Run modes

The GitHub Actions workflow runs on two off-peak schedules (UTC):
- nightly: 18:17 UTC (≈ 11:47 PM IST), timeout 120 min
- weekly: 20:28 UTC on Sunday (≈ Monday 02:58 AM IST), timeout 240 min

Manual runs:
- From the Actions tab, click “Run workflow”.
- Optional input force_mode accepts nightly or weekly to test a specific mode.
- Manual runs do not push to main by default unless they are scheduled; artifacts are always uploaded for inspection.

### Artifacts and health

Each run uploads:
- data.json
- health.json

Artifacts are retained for 7 days. The homepage displays a status banner that reads health.json to show Health, Last Updated, and Listings count.

### Configuration knobs

Set optional tunables as Actions → Variables (safe defaults exist in code):
- MAX_WORKERS, PER_SOURCE_MAX
- CONNECT_TIMEOUT, READ_TIMEOUT
- RETRY_TOTAL, RETRY_CONNECT, RETRY_READ, BACKOFF_FACTOR, MAX_BACKOFF_SECONDS
- PER_HOST_RPM, BASELINE_SLEEP_S, JITTER_MIN, JITTER_MAX
- CB_FAILURE_THRESHOLD, CB_OPEN_SECONDS, CB_HALF_OPEN_PROBE

Sensitive values:
- TELEGRAM_RSS_BASE must be set as a Secret (Actions → Secrets and variables → Secrets).

### Verification and filters

- Verified when: official-domain link present (source=official, verifiedBy=official), or 2+ distinct aggregators corroborate (source=aggregator, verifiedBy=multi-aggregator).
- Education: includes 10th/Matric, 12th/Intermediate, Any Graduate; excludes PG/professional (Master’s, MBA/PGDM, Law, Engineering, MCA/BCA, CA/CFA/CS/CMA, MBBS/BDS/Nursing/Pharm, PhD).
- Skills allowed: typing/steno, CCC/DOEACC/NIELIT/O-level, basic computer knowledge, PET/PST.
- Tech/tool exclusion: Python/Java/AutoCAD/Matlab/SAP unless Any Graduate explicitly acceptable.
- Domicile: includes All India and Bihar-only; excludes other state-only restricted posts.
