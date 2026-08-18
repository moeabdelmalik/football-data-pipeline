# Requirements — Football Results & Standings Pipeline

**Status:** Approved · **Version:** 1.0 · **Owner:** Data Engineering
**Source:** TheSportsDB REST API → GCS → BigQuery → dbt

---

## 1. Purpose

Deliver a trusted, queryable view of European football results that supports
**league standings and team form analysis** across multiple leagues and seasons.

This is a portfolio project. Success is measured not only by working code but by
demonstrating four engineering practices explicitly: incremental & idempotent
loading, dimensional modelling, data quality testing, and orchestration.

---

## 2. Stakeholders & use cases

| Consumer | Question they need answered |
|---|---|
| Football analyst | What is the current league table? Points, GD, W/D/L per team. |
| Football analyst | What is a team's form over its last 5 matches? |
| Analyst / BI tool | How does a team's home record compare to its away record? |
| Analyst / BI tool | How do standings evolve week by week through a season? |

**Explicitly out of scope (v1):** player-level statistics, live in-match updates,
betting odds, expected-goals or other derived metrics not present in the source.

---

## 3. Source system analysis

**API:** `https://www.thesportsdb.com/api/v1/json/{api_key}/`
**Auth:** API key embedded in the URL path. Public test key `3` works unauthenticated.
**Pagination:** None — endpoints return a complete JSON array under a single root key.

### Endpoints in scope

| Endpoint | Root key | Purpose | Verified |
|---|---|---|---|
| `all_leagues.php` | `leagues` | League reference data | ✅ 3 fields |
| `lookupleague.php?id=` | `leagues` | League detail + current season | ✅ |
| `lookup_all_teams.php?id={league}` | `teams` | Team dimension | ✅ 64 fields, 24 rows for EPL |
| `eventsseason.php?id={league}&s={season}` | `events` | Match facts | ✅ 30 fields |

### Verified constraints

- **CONSTRAINT-1 — Result cap on the test key.** Key `3` truncates event endpoints
  to ~5 rows per call (a full season is 306–380 matches). Team endpoints return
  complete results. *Impact:* ingestion must fan out across league × season to
  build volume. A patron key removes the cap with no code change.
- **CONSTRAINT-2 — No pagination or cursor.** Each call returns a whole season.
  There is no server-side incremental filter; incrementality must be enforced
  on our side, at load time.
- **CONSTRAINT-3 — Mutable current season.** Season `2026-2027` is in progress.
  Fixture rows exist before kickoff with `intHomeScore`/`intAwayScore` as `null`,
  and are updated in place after the match. *Impact:* loads must MERGE on
  `idEvent`, never APPEND.
- **CONSTRAINT-4 — Untyped payloads.** Every field arrives as a JSON string,
  including scores and dates. Casting is a transform-layer responsibility.
- **CONSTRAINT-5 — Undocumented rate limits.** No published quota and no
  rate-limit headers observed. Assume fragility: throttle client-side and
  retry with exponential backoff.

### Scope of extraction

**Leagues (5):**

| ID | League | Country |
|---|---|---|
| 4328 | English Premier League | England |
| 4331 | German Bundesliga | Germany |
| 4332 | Italian Serie A | Italy |
| 4334 | French Ligue 1 | France |
| 4335 | Spanish La Liga | Spain |

**Seasons (3):** `2024-2025` (final) · `2025-2026` (final) · `2026-2027` (in progress)

**Expected volume at full key:** ~1,750 matches/season × 3 ≈ **5,250 matches**,
producing ≈ **10,500 rows** in the team-match fact table. Trivial for BigQuery;
comfortably inside the free tier.

---

## 4. Target data model

**Grain of the core fact table: one row per team, per match.**

Each source match produces **two** fact rows — one from the home team's
perspective, one from the away team's. This is what makes standings a simple
`GROUP BY team` aggregation instead of an awkward union of home and away columns.

```
                  dim_league
                       │
   dim_date ───── fct_team_match ───── dim_team
                       │
              (self-join: opponent)
```

| Table | Grain | Notes |
|---|---|---|
| `fct_team_match` | team × match | Core fact. 2 rows per match. |
| `fct_match` | match | Intermediate; one row per fixture. |
| `dim_team` | team | SCD Type 1 in v1. |
| `dim_league` | league | Small, static. |
| `dim_date` | day | Generated, not sourced. |

**`fct_team_match` measures:** `goals_for`, `goals_against`, `goal_difference`,
`points` (3/1/0), `is_win`, `is_draw`, `is_loss`, `is_home`, `is_played`.

**Marts:** `mart_league_standings` (aggregated table), `mart_team_form`
(rolling 5-match window).

---

## 5. Non-functional requirements

| # | Requirement | Target |
|---|---|---|
| NFR-1 | Freshness | Weekly. Matches Europe's weekly fixture rounds. |
| NFR-2 | Schedule | Mondays 03:00 UTC — after weekend fixtures conclude. |
| NFR-3 | Idempotency | Re-running any period produces identical results. No duplicates. |
| NFR-4 | Backfill | Any league × season reloadable independently, without full refresh. |
| NFR-5 | Raw retention | Raw NDJSON in GCS retained indefinitely; partitioned by ingest date. |
| NFR-6 | Cost | Within BigQuery free tier (< 1 TB scanned/month). |
| NFR-7 | Recovery | A failed run recovers on retry without manual cleanup. |
| NFR-8 | Secrets | No credential in source control or container image. |

---

## 6. Data quality contract

Enforced as dbt tests. **A failure blocks promotion to marts.**

### Critical — pipeline fails

| ID | Rule | Layer |
|---|---|---|
| DQ-1 | `idEvent` is unique and not null | staging |
| DQ-2 | Every match has exactly 2 rows in `fct_team_match` | fact |
| DQ-3 | `team_key` and `league_key` have referential integrity to their dimensions | fact |
| DQ-4 | `points` ∈ {0, 1, 3} | fact |
| DQ-5 | A team never plays itself (`team_id != opponent_id`) | fact |

### Warning — logged, does not block

| ID | Rule |
|---|---|
| DQ-6 | Played matches have non-null, non-negative scores |
| DQ-7 | Unplayed fixtures have null scores |
| DQ-8 | `goals_for` ≤ 20 (outlier detection) |
| DQ-9 | Match dates fall within their stated season window |
| DQ-10 | Source freshness: current season updated within the last 8 days |

---

## 7. Success criteria

1. `docker compose up` produces a running Airflow with the DAG visible.
2. A full backfill loads all 5 leagues × 3 seasons without manual intervention.
3. **Running the DAG twice produces identical row counts** — the idempotency proof.
4. All critical dbt tests pass; `dbt docs` renders the lineage graph.
5. `mart_league_standings` reproduces a published league table for a finished season.
6. A deliberately corrupted row causes a visible, specific test failure.

---

## 8. Open questions / assumptions

- **A1:** Test key `3` assumed for development. Volume will be a small sample
  until a patron key is supplied. Pipeline logic is unaffected.
- **A2:** `dim_team` is SCD Type 1 (overwrite). Team attributes such as stadium
  change rarely; history is not required for standings.
- **A3:** Points assumed 3/1/0 for all five leagues — true for all in scope.
- **Q1:** Should postponed matches (`strPostponed = 'yes'`) be excluded from
  standings, or carried with null scores? *Proposed: carry, flag `is_played = false`.*

---

## 9. Delivery phases

| Phase | Deliverable | Status |
|---|---|---|
| 0 | Requirements & repo scaffolding | ✅ Complete |
| 1 | Extraction layer — config-driven, retrying, rate-limited | Next |
| 2 | Load layer — GCS landing → BigQuery raw, MERGE-based |  |
| 3 | Transform — dbt staging → marts, with tests |  |
| 4 | Orchestration — Airflow DAG, dynamic task mapping |  |
| 5 | Containerisation & documentation |  |
