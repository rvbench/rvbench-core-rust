# Observability contract 1.0.0

Approved 2026-10-17, including the four-tab console. This contract precedes implementation.
HTTP authority: both service OpenAPI files; shared generated types: openapi.json.
No authentication, allow arbitrary valid worker registration; observability is optional.

- `/metrics` is Prometheus text 0.0.4. Scrapes read a committed in-memory projection,
  never read/deserialize the SQLite job spool. Projection publication follows commit.
- Business submission/completion counters are reconstructed from the durable, retained
  job ledger. Jobs are not deleted. Any future pruning must migrate cumulative counts.
  Replays and restarts cannot increase counts. Historical phase timings are omitted.
- Manager logical jobs and worker executions are separate namespaces. `origin` is
  direct/managed. ACK is not actual running. Assignment phases pending_ack,
  acknowledged, uncertain are mutually exclusive, excluding terminal jobs.
- Online expires after 30 seconds without registration; it is distinct from ready,
  drained, and scrape up. Offline discovery targets persist until explicit retirement.
  Collection health includes per-target up/error and last-scrape timestamps. A healthy
  VictoriaMetrics process with failing exporters/workers is degraded, not fully healthy.
- Stable series labels: board_id/worker_id/backend/kind/origin/state and finite phase,
  outcome, route, method, status enums. Never job/attempt/instance/lease IDs or text.
- Host discovery deduplicates board_id; conflicting exporter URLs are omitted with a
  collection error, rather than picking an arbitrary worker's declaration.
- Range uses a fixed metric catalog, 30..3600s steps, positive finite timestamps,
  start <= end, at most 24h and 1800 points. Labels are escaped, response bounded.
  Missing points omit value (UI gap); NaN/Inf never become zero. Last sample age >90s
  marks stale. Query failures return a partial response with errors and no fake series.
- Results are paginated over the entire ledger, newest first, exact filters plus text
  search. Full source/logs remain in `/jobs/{id}`, not in list responses.
  Manager submitted_at is the original logical admission time; terminal worker events
  cannot replace it with local admission time. Worker timestamps remain local.
- Rankings include only done performance jobs with passed compiler/evaluator,
  artifacts, in-run `correct=true`, finite positive samples matching repetitions,
  and compilation_excluded=true. Earlier unverified timings remain results only.
  Median is calculated from samples, not accepted from arbitrary metadata.
- Groups bind physical board, backend, op/shape/dtype/seed, warmup/repetitions,
  clock, runtime/compiler/CPU configuration fingerprint, and harness digest.
  Each implementation's latest eligible run participates, sorted by median ascending,
  job_id breaks ties. No cross-group/global score. Show exclusion counts.
- Speedup is optional, only from same-run native-reference samples, with median
  recomputed, matching repetitions. Historical suite baseline results are not mixed in.
- Performance evaluator now verifies outputs before timing; preflight work is excluded
  from samples. `func` remains compilation plus smoke, not numerical verification.
- Phase times are wall seconds, compiler/evaluator separately; absent means unknown.
  Persist once with the terminal record/outbox. Queue wait uses actual worker start.
- UI has system status (home), submission, results and leaderboard tabs. HTML/CSS/JS
  remain self-contained. Standalone worker keeps all four local views; VM optional.
  Unknown/missing/stale states are visible. Metric outages cannot block job execution.
