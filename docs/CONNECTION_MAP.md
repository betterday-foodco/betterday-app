# BetterDay Corporate App — Connection Map

> **What this is:** A plain-English inventory of every "connection point" in the BetterDay corporate app infrastructure — every place that data flows between two systems. Written for non-technical readers as well as developers, so anyone can scan it and understand the moving parts.
>
> **Last reviewed:** 2026-04-07
>
> **Maintainer:** Conner. Update this doc whenever a connection is added, removed, or has a status change (LIVE / DORMANT / DEAD / PLANNED).

---

## The big picture

There are five main systems involved in running the corporate app today:

1. **User browsers** — employees, managers, and admins
2. **betterday-app** — the Flask app, hosted on Render
3. **Google Apps Script + Google Sheets** — the current backend (being retired)
4. **culinary-ops** — the new NestJS backend, hosted on Railway (gradually taking over)
5. **PostgreSQL** — the database, hosted on Railway

Plus a handful of external services (email providers, GitHub, etc.) and one collaboration channel (the collab board).

### Visual snapshot of today

```
                    ┌──────────────────────┐
                    │   USER BROWSERS      │
                    │  (employees,         │
                    │   managers, admins)  │
                    └──────────┬───────────┘
                               │
                               │ HTTPS
                               ▼
                    ┌──────────────────────┐
                    │   betterday-app      │
                    │   (Flask)            │
                    │   Hosted on Render   │
                    └─┬──────────┬─────┬───┘
                      │          │     │
            (1) GAS calls   (2) DEAD  (3) DORMANT
                      │       pipe     adapter
                      ▼          │     │
            ┌──────────────┐     │     │
            │ Google Apps  │     │     │
            │ Script (GAS) │     │     │
            └──────┬───────┘     │     │
                   │             │     │
                   ▼             │     │
            ┌──────────────┐     │     │
            │ Google       │     ▼     ▼
            │ Sheets       │  ┌──────────────────┐
            │ (Buffer)     │  │ culinary-ops     │
            └──────────────┘  │ (NestJS)         │
                              │ Railway hosted   │
                              └────────┬─────────┘
                                       │
                                       ▼
                              ┌──────────────────┐
                              │ PostgreSQL       │
                              │ Railway hosted   │
                              └──────────────────┘
```

---

## Status legend

| Status | Meaning |
|---|---|
| **LIVE** | Currently in production, used every day |
| **DORMANT** | Code exists but feature-flagged off; will activate later |
| **DEAD** | Code exists but nothing actually calls it |
| **DEPRECATED** | Used to be live; replaced by something else but not deleted yet |
| **PLANNED** | Not built yet, on the roadmap |
| **CONFIG** | Not a runtime data flow — supporting config (env var, secret, etc.) |

---

## The full list

### A. User → App connections (the front door)

| # | Connection | Status | What it does |
|---|---|---|---|
| 1 | Employee browser → Flask | **LIVE** | Employee logs in via magic link, browses menu, places weekly order |
| 2 | Manager browser → Flask `/manager/auth` | **LIVE** | Manager enters company PIN to log in |
| 3 | Manager browser → Flask `/manager/dashboard` | **LIVE** | Manager sees employees, orders, invoices, par levels for their company |
| 4 | Admin browser → Flask `/admin/*` | **LIVE** | Conner manages all companies, invoices, generates reports |
| 5 | Anyone → Flask `/api/gas` proxy | **LIVE** | Generic catch-all that forwards arbitrary requests to GAS |

### B. Flask → Google Apps Script (the current backend)

| # | Connection | Status | What it does |
|---|---|---|---|
| 6 | Flask `_gas_post()` → GAS web app URL | **LIVE** | Every read/write of corporate data flows through here. ~30 routes use it. |
| 7 | GAS → Google Sheets (read) | **LIVE** | GAS reads tabs: Companies, Employees, BenefitLevels, ParLevels, CorporateOrders, Invoices, etc. |
| 8 | GAS → Google Sheets (write) | **LIVE** | GAS writes new orders, updates employees, logs invoice changes |
| 9 | GAS → Google MailApp | **LIVE** | GAS sends magic link emails to employees and managers |

### C. Flask → culinary-ops (the new backend)

| # | Connection | Status | What it does |
|---|---|---|---|
| 10 | Flask `culinary_ops_client.get/post/...` → culinary-ops API | **DORMANT** | The HTTP adapter shipped in PR #3. Sits behind `CULINARY_OPS_ENABLED` flag. Will become the primary path after Phase 1 cutover. |
| 11 | Flask reads `session['culinary_ops_jwt']` for auth header | **DORMANT** | Phase 2 (auth migration) will write the JWT here after a manager logs in |

### D. culinary-ops → external services

| # | Connection | Status | What it does |
|---|---|---|---|
| 12 | culinary-ops NestJS → PostgreSQL via Prisma | **LIVE (kitchen) / DORMANT (corporate)** | All recipe/production data is live. The 12 `Corporate*` tables are built but mostly empty until Phase 1 runs. |
| 13 | culinary-ops → Resend (email API) | **LIVE** | Used for non-corporate emails today. Will send corporate magic links after Phase 2. |
| 14 | culinary-ops corporate endpoints → PostgreSQL | **DORMANT** | The 26 endpoints in PR #7 — built, tested, waiting on PR #7 merge + `prisma db push`. |

### E. The "internal sync" pipes (mostly dead)

| # | Connection | Status | What it does |
|---|---|---|---|
| 15 | culinary-ops → Flask `GET /api/internal/orders` | **DEPRECATED** | Used to be how culinary-ops pulled aggregated order data for production planning. culinary-ops rewrote its side to read from its own database, so nothing calls this anymore. |
| 16 | (anyone) → Flask `POST /api/internal/menu` | **DEAD** | Was supposed to let culinary-ops push the published menu into Flask's cache. Was never called. |
| 17 | Both protected by `CULINARY_SYNC_KEY` shared secret | **CONFIG** | The header-based auth on the two pipes above. Still in place even though pipes are unused. |

### F. Direct culinary-ops user-facing connections (Gurleen's side)

| # | Connection | Status | What it does |
|---|---|---|---|
| 18 | Kitchen staff browser → culinary-ops admin UI | **LIVE** | Gurleen and her team manage recipes, production plans, ingredients directly. Doesn't touch corporate data. |
| 19 | Manager browser → culinary-ops directly | **NOT PLANNED** | All corporate user traffic stays going through Flask. No plan to have managers hit culinary-ops directly. |

### G. Hosting / infrastructure connections

| # | Connection | Status | What it does |
|---|---|---|---|
| 20 | Render → betterday-app (deploy) | **LIVE** | Pushes to `main` branch trigger Render to rebuild and redeploy Flask |
| 21 | Railway → culinary-ops (deploy) | **LIVE** | Pushes to culinary-ops main trigger Railway to rebuild NestJS |
| 22 | Railway → PostgreSQL (managed database) | **LIVE** | Railway runs the Postgres instance, handles backups, etc. Billing comes to Conner. |
| 23 | GitHub → both repos | **LIVE** | Source of truth for all code. Conner + Claude + Gurleen push commits here. |

### H. Collab board (meta — not part of running infrastructure)

| # | Connection | Status | What it does |
|---|---|---|---|
| 24 | Both Claude sessions → `betterday-collab/data.json` on GitHub | **LIVE** | The shared status board visible at https://betterday-foodco.github.io/collab/. Both Conner's and Gurleen's Claude sessions update this. |

---

## Configuration / secrets at each boundary

These are the env vars and shared secrets that wire the connections together. Knowing where they live is important if anything ever needs to be rotated.

| Secret / config | Lives in | Used by |
|---|---|---|
| `GOOGLE_SCRIPT_URL` | Render env (betterday-app) | Connection #6 — Flask → GAS proxy URL |
| `CULINARY_SYNC_KEY` | Render env + Railway env | Connections #15, #16 — internal sync pipes (currently unused) |
| `CULINARY_OPS_BASE_URL` | Render env (betterday-app) | Connection #10 — adapter base URL |
| `CULINARY_OPS_ENABLED` | Render env (betterday-app) | Connection #10 — feature flag (currently off) |
| `DATABASE_URL` | Railway env (culinary-ops) | Connections #12, #14 — Postgres connection string |
| JWT secret | Railway env (culinary-ops) | Connection #11 — manager JWT signing key (Phase 2+) |
| Resend API key | Railway env (culinary-ops) | Connection #13 — email sending |

---

## What's about to change (phase by phase)

The goal of the migration is to retire the GAS column entirely and replace it with the culinary-ops column. Here's how the connection list shifts as the migration runs:

| Phase | Connections that turn ON | Connections that turn OFF |
|---|---|---|
| **Phase 1 (read-only routes)** | #10 partially activated for `/manager/dashboard` reads | None — GAS stays as the fallback |
| **Phase 2 (auth)** | #11 activated; #13 starts sending corporate emails | #9 (GAS MailApp magic links) starts being unused |
| **Phase 3 (employee/benefit CRUD)** | #10 takes over employee management | Corresponding GAS actions in #6 fall idle |
| **Phase 4 (orders/par levels)** | #10 takes over order submission | Order-related GAS actions in #6 fall idle |
| **Phase 5 (invoicing)** | #10 takes over invoicing | Invoice-related GAS actions in #6 fall idle |
| **Phase 6 (cleanup)** | None new | #6, #7, #8, #9 all retired. GAS deleted. Sheets archived. |

### After Phase 6 (target end state)

```
                    ┌──────────────────────┐
                    │   USER BROWSERS      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   betterday-app      │
                    │   (Flask, thin)      │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   culinary-ops       │
                    │   (NestJS)           │
                    └──────────┬───────────┘
                               │
                               ▼
                    ┌──────────────────────┐
                    │   PostgreSQL         │
                    │   (single source     │
                    │    of truth)         │
                    └──────────────────────┘
```

Three runtime hops instead of today's five-plus. One database instead of GAS+Sheets+Postgres. One source of truth.

---

## Key observations

A few things worth noting that aren't obvious from the table alone:

**1. Today, GAS is a single point of failure.** Connections #6, #7, #8, and #9 are the entire backbone of the corporate app right now. If GAS goes down or hits its quota, every manager dashboard and every employee order breaks. After migration, that whole column disappears and gets replaced by the more reliable Postgres pathway.

**2. The two "internal sync" pipes (#15, #16) are tech debt.** They're code that's still wired up but does nothing useful. They'll get cleaned up in Phase 6. Worth knowing about so nobody is confused when they're deleted.

**3. The DORMANT connections (#10, #11, #14) are the migration runway.** They're already built — they just need flags flipped. That's why the cutover can happen quickly once PR #7 merges and the database is updated.

**4. After migration, the corporate app has only one runtime data dependency: culinary-ops → PostgreSQL.** Right now it depends on Flask, GAS, Sheets, and the dormant adapter. Simpler is more reliable.

**5. Conner owns every link in the chain.** Render (Flask hosting), Railway (PostgreSQL + culinary-ops hosting), GitHub (code), and the Google account that owns Sheets and GAS — all on accounts Conner controls. Nothing is hosted on someone else's infrastructure.

**6. Email delivery is worth tracking explicitly.** Magic link delivery is critical to login working. Today it goes through Google MailApp (free, attached to the Google Workspace). After Phase 2, it goes through Resend. If Resend ever has an outage, manager logins break. The Resend account/API key ownership should be documented separately.

---

## Questions to answer before Phase 1 cutover

These are the open items where the connection map has a question mark today:

- [ ] Confirm production `CULINARY_OPS_BASE_URL` (default in code: `https://culinary-ops-backend-production.up.railway.app`)
- [ ] Confirm Resend account ownership (Conner's vs. Gurleen's) for Phase 2 email delivery
- [ ] Document nightly database backup destination (Railway built-in is on; verify off-Railway copy exists)
- [ ] Confirm the JWT session field name (`session['culinary_ops_jwt']`) so Phase 2 writes match what Phase 1 reads
- [ ] Verify Postgres user roles — is Gurleen using master `postgres` user, or a scoped service account?

---

## Update log

| Date | Change | By |
|---|---|---|
| 2026-04-07 | Initial document created | Conner / Claude |
