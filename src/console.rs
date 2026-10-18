//! Committed read projection shared by standalone worker and manager consoles.
//! Wire models are generated; this module contains internal indexes only.
use crate::*;
use axum::{
    extract::{MatchedPath, Request},
    middleware::Next,
};
use std::{collections::BTreeMap, sync::RwLock};

#[derive(Clone)]
pub struct LeaseView {
    pub worker: String,
    pub acknowledged: bool,
    pub deadline: f64,
}
#[derive(Default)]
pub struct View {
    pub results: Vec<ResultEntry>,
    pub ranking: Vec<RankingGroup>,
    pub excluded: i64,
    pub workers: Vec<ObservedWorker>,
    pub leases: Vec<LeaseView>,
    pub counts: Vec<CountEntry>,
    pub outbox: usize,
    pub oldest: f64,
    pub metrics: String,
    pub updated: f64,
}
pub struct Console {
    pub service: &'static str,
    pub view: RwLock<Arc<View>>,
    pub http: Mutex<BTreeMap<(String, String, u16), u64>>,
    pub started: f64,
    pub identity: Option<(String, String, String)>,
}
pub fn escape(v: &str) -> String {
    v.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
}
fn sample(out: &mut String, name: &str, labels: &str, value: f64) {
    if value.is_finite() {
        out.push_str(&format!("{name}{{{labels}}} {value}\n"));
    }
}
pub fn timestamp(s: &str) -> Option<f64> {
    chrono::DateTime::parse_from_rfc3339(s)
        .ok()
        .map(|v| v.timestamp_millis() as f64 / 1000.)
}
pub fn median(mut v: Vec<f64>) -> Option<f64> {
    if v.is_empty() || v.iter().any(|x| !x.is_finite() || *x <= 0.) {
        return None;
    }
    v.sort_by(f64::total_cmp);
    let n = v.len();
    Some(if n.is_multiple_of(2) {
        (v[n / 2 - 1] + v[n / 2]) / 2.
    } else {
        v[n / 2]
    })
}
fn samples(details: &Value, name: &str, n: i64) -> Option<f64> {
    let a = details[name].as_array()?;
    if a.len() != n as usize {
        return None;
    }
    median(a.iter().map(Value::as_f64).collect::<Option<Vec<_>>>()?)
}
impl Console {
    pub fn new(service: &'static str, identity: Option<(String, String, String)>) -> Arc<Self> {
        Arc::new(Self {
            service,
            identity,
            view: RwLock::new(Arc::new(View::default())),
            http: Mutex::new(BTreeMap::new()),
            started: seconds(),
        })
    }
    pub fn snapshot(&self) -> Arc<View> {
        self.view.read().unwrap().clone()
    }
    pub fn update(
        &self,
        jobs: Vec<(&JobRecord, &str)>,
        workers: Vec<ObservedWorker>,
        leases: Vec<LeaseView>,
        outbox: usize,
        oldest: f64,
    ) {
        let mut v = View {
            workers,
            leases,
            outbox,
            oldest,
            updated: seconds(),
            ..View::default()
        };
        let mut counts = BTreeMap::<(String, String, String, String), u64>::new();
        let backends: Vec<&str> = self
            .identity
            .as_ref()
            .map(|i| vec![i.2.as_str()])
            .unwrap_or(vec!["k1", "k3"]);
        for b in backends {
            for k in ["func", "correctness", "performance"] {
                for s in ["queued", "running", "done", "failed", "timeout", "canceled"] {
                    for o in ["direct", "managed"] {
                        counts.insert((b.into(), k.into(), s.into(), o.into()), 0);
                    }
                }
            }
        }
        let base = self
            .identity
            .as_ref()
            .map(|(w, b, _)| format!("worker_id=\"{}\",board_id=\"{}\",", escape(w), escape(b)))
            .unwrap_or_default();
        let prefix = format!("rvbench_{}", self.service);
        let mut hist = BTreeMap::<(String, String, String, String), Vec<f64>>::new();
        let mut groups = BTreeMap::<String, (Value, BTreeMap<String, (String, Value)>)>::new();
        for (r, origin) in jobs {
            *counts
                .entry((
                    r.request.backend.to_string(),
                    r.request.kind.to_string(),
                    r.state.to_string(),
                    origin.into(),
                ))
                .or_default() += 1;
            if let Some(t) = &r.phase_timings_seconds {
                for (phase, duration) in [
                    ("compiler", t.compiler),
                    ("evaluator", t.evaluator),
                    ("queue_wait", t.queue_wait),
                ] {
                    if let Some(x) = duration.filter(|x| x.is_finite() && *x >= 0.) {
                        let status = match phase {
                            "compiler" => r.compilation.as_ref(),
                            "evaluator" => r.evaluation.as_ref(),
                            _ => None,
                        };
                        let outcome = if phase == "queue_wait" {
                            r.state.as_str()
                        } else if status.is_some_and(|p| p.status == PhaseStatus::Passed) {
                            "passed"
                        } else if r.state == JobState::Timeout {
                            "timeout"
                        } else {
                            "failed"
                        };
                        hist.entry((
                            phase.into(),
                            r.request.kind.to_string(),
                            outcome.into(),
                            origin.into(),
                        ))
                        .or_default()
                        .push(x);
                    }
                }
            }
            let eval = r
                .evaluation
                .as_ref()
                .map(|p| json!(p.details))
                .unwrap_or(json!({}));
            let ms = samples(&eval, "samples_ms", r.request.repetitions);
            let reference = samples(&eval, "reference_samples_ms", r.request.repetitions);
            let implementation = r
                .compilation
                .as_ref()
                .and_then(|p| {
                    p.artifacts
                        .iter()
                        .find(|a| a.path == "candidate.c" || a.path == "implementation.py")
                })
                .map(|a| a.sha256.clone());
            let mut row = json!({"job_id":r.job_id,"submitted_at":r.submitted_at,"backend":r.request.backend,"kind":r.request.kind,"state":r.state,"op":r.request.op,"shape":r.request.shape,"dtype":r.request.dtype,"worker_id":r.worker_id,"board_id":r.board_id,"source_sha256":implementation.clone().unwrap_or_else(||digest(&r.request.source)),"compilation_status":r.compilation.as_ref().map(|p|p.status.as_str()).unwrap_or("unknown"),"evaluation_status":r.evaluation.as_ref().map(|p|p.status.as_str()).unwrap_or("unknown")});
            row["implementation"] = json!(crate::implementation(&r.request));
            if let Some(x) = ms {
                row["median_ms"] = json!(x);
            }
            if let Some(x) = reference {
                row["reference_median_ms"] = json!(x);
            }
            if let Some(x) = eval["correct"].as_bool() {
                row["correct"] = json!(x);
            }
            if let Some(x) = &r.error {
                row["error"] = json!(x);
            }
            v.results.push(serde_json::from_value(row.clone()).unwrap());
            if r.request.kind != JobKind::Performance {
                continue;
            }
            let eligible = r.state == JobState::Done
                && r.compilation
                    .as_ref()
                    .is_some_and(|p| p.status == PhaseStatus::Passed && !p.artifacts.is_empty())
                && r.evaluation
                    .as_ref()
                    .is_some_and(|p| p.status == PhaseStatus::Passed && !p.artifacts.is_empty())
                && eval["correct"] == true
                && eval["compilation_excluded"] == true
                && ms.is_some()
                && implementation.is_some();
            let Some(comp) = &r.compilation else {
                v.excluded += 1;
                continue;
            };
            let details = json!(comp.details);
            let mut env = details["environment"].clone();
            if let Some(env) = env.as_object_mut() {
                env.remove("probe_compilation");
                if let Some(modes) = env
                    .get_mut("implementations")
                    .and_then(Value::as_object_mut)
                {
                    for mode in modes.values_mut().filter_map(Value::as_object_mut) {
                        mode.remove("probe_compilation");
                    }
                }
            }
            let harness = comp
                .artifacts
                .iter()
                .find(|a| a.path == "harness.c" || a.path == "harness.py");
            if !eligible || !env.is_object() || harness.is_none() || !eval["clock"].is_string() {
                v.excluded += 1;
                continue;
            }
            let env_hash = digest(&env);
            let context = json!({"board_id":r.board_id,"backend":r.request.backend,"implementation":crate::implementation(&r.request),"op":r.request.op,"shape":r.request.shape,"dtype":r.request.dtype,"seed":r.request.seed,"warmup":r.request.warmup,"repetitions":r.request.repetitions,"environment_sha256":env_hash,"clock":eval["clock"],"harness_sha256":harness.unwrap().sha256});
            let group_id = digest(&context);
            let (mut group, latest) = groups
                .remove(&group_id)
                .unwrap_or_else(|| (context.clone(), BTreeMap::new()));
            group.as_object_mut().unwrap().remove("harness_sha256");
            group["group_id"] = json!(group_id);
            let mut latest = latest;
            let key = implementation.unwrap();
            let ordering = format!("{}|{}", r.submitted_at, r.job_id);
            if latest.get(&key).is_none_or(|(at, _)| ordering > *at) {
                let mut ranked = json!({"job":row,"rank":0});
                if let (Some(a), Some(b)) = (reference, ms) {
                    ranked["speedup"] = json!(a / b);
                }
                latest.insert(key, (ordering, ranked));
            }
            groups.insert(group_id, (group, latest));
        }
        v.results.sort_by(|a, b| {
            b.submitted_at
                .cmp(&a.submitted_at)
                .then(b.job_id.cmp(&a.job_id))
        });
        for (_, (mut group, entries)) in groups {
            let mut rows: Vec<Value> = entries.into_values().map(|(_, r)| r).collect();
            rows.sort_by(|a, b| {
                a["job"]["median_ms"]
                    .as_f64()
                    .unwrap()
                    .total_cmp(&b["job"]["median_ms"].as_f64().unwrap())
                    .then(
                        a["job"]["job_id"]
                            .as_str()
                            .cmp(&b["job"]["job_id"].as_str()),
                    )
            });
            for (i, r) in rows.iter_mut().enumerate() {
                r["rank"] = json!(i + 1);
            }
            group["entries"] = json!(rows);
            v.ranking.push(serde_json::from_value(group).unwrap());
        }
        let mut submitted = BTreeMap::<(String, String, String), u64>::new();
        for ((b, k, s, o), n) in counts {
            v.counts.push(
                serde_json::from_value(json!({"kind":k,"state":s,"origin":o,"count":n})).unwrap(),
            );
            let labels = format!("{base}backend=\"{b}\",kind=\"{k}\",state=\"{s}\",origin=\"{o}\"");
            sample(&mut v.metrics, &format!("{prefix}_jobs"), &labels, n as f64);
            if !["queued", "running"].contains(&s.as_str()) {
                sample(
                    &mut v.metrics,
                    &format!("{prefix}_jobs_completed_total"),
                    &labels,
                    n as f64,
                );
            }
            *submitted.entry((b, k, o)).or_default() += n;
        }
        for ((b, k, o), n) in submitted {
            sample(
                &mut v.metrics,
                &format!("{prefix}_jobs_submitted_total"),
                &format!("{base}backend=\"{b}\",kind=\"{k}\",origin=\"{o}\""),
                n as f64,
            );
        }
        for ((phase, kind, outcome, origin), values) in hist {
            let name = if phase == "queue_wait" {
                format!("{prefix}_queue_wait_seconds")
            } else {
                format!("{prefix}_phase_duration_seconds")
            };
            let labels = format!(
                "{base}component=\"{phase}\",kind=\"{kind}\",outcome=\"{outcome}\",origin=\"{origin}\""
            );
            for le in [
                0.01,
                0.05,
                0.1,
                0.5,
                1.,
                5.,
                10.,
                30.,
                60.,
                300.,
                1800.,
                f64::INFINITY,
            ] {
                let bucket = if le.is_infinite() {
                    "+Inf".into()
                } else {
                    le.to_string()
                };
                sample(
                    &mut v.metrics,
                    &format!("{name}_bucket"),
                    &format!("{labels},le=\"{bucket}\""),
                    values.iter().filter(|v| **v <= le).count() as f64,
                );
            }
            sample(
                &mut v.metrics,
                &format!("{name}_sum"),
                &labels,
                values.iter().sum(),
            );
            sample(
                &mut v.metrics,
                &format!("{name}_count"),
                &labels,
                values.len() as f64,
            );
        }
        *self.view.write().unwrap() = Arc::new(v);
    }
    pub fn workers(&self, v: &View) -> Vec<ObservedWorker> {
        let now = seconds();
        let mut ws = v.workers.clone();
        for w in &mut ws {
            if self.service == "worker" {
                w.last_seen = now;
            }
            w.online = now - w.last_seen <= 30.;
            w.pending_ack = 0;
            w.acknowledged = 0;
            w.uncertain = 0;
            for l in v.leases.iter().filter(|l| l.worker == w.worker.worker_id) {
                if l.deadline < now {
                    w.uncertain += 1;
                } else if l.acknowledged {
                    w.acknowledged += 1;
                } else {
                    w.pending_ack += 1;
                }
            }
        }
        ws
    }
    pub fn summary(&self, health: CollectionHealth) -> ObservationSummary {
        let v = self.snapshot();
        serde_json::from_value(json!({"schema_version":"1.0.0","generated_at":seconds(),"service":self.service,"counts":v.counts,"workers":self.workers(&v),"outbox_pending":v.outbox,"outbox_oldest_age_seconds":if v.oldest>0.{(seconds()-v.oldest).max(0.)}else{0.},"collection":health})).unwrap()
    }
    pub fn metrics(&self) -> String {
        let v = self.snapshot();
        let mut out = v.metrics.clone();
        let base = self
            .identity
            .as_ref()
            .map(|(w, b, _)| format!("worker_id=\"{}\",board_id=\"{}\"", escape(w), escape(b)))
            .unwrap_or_default();
        sample(&mut out, "process_start_time_seconds", &base, self.started);
        sample(
            &mut out,
            "rvbench_scrape_cache_updated_timestamp_seconds",
            &base,
            v.updated,
        );
        sample(&mut out, &format!("rvbench_{}_up", self.service), &base, 1.);
        if self.service == "worker" {
            sample(
                &mut out,
                "rvbench_worker_outbox_pending",
                &base,
                v.outbox as f64,
            );
            sample(
                &mut out,
                "rvbench_worker_outbox_oldest_age_seconds",
                &base,
                if v.oldest > 0. {
                    (seconds() - v.oldest).max(0.)
                } else {
                    0.
                },
            );
        }
        for w in self.workers(&v) {
            let labels = format!(
                "worker_id=\"{}\",board_id=\"{}\",backend=\"{}\"",
                escape(&w.worker.worker_id),
                escape(&w.worker.board_id),
                w.worker.backend
            );
            for (key, num) in [
                ("online", w.online as u8 as f64),
                ("ready", w.worker.ready as u8 as f64),
                ("drained", w.drained as u8 as f64),
                ("last_seen_timestamp_seconds", w.last_seen),
                ("capacity", w.worker.capacity as f64),
            ] {
                sample(
                    &mut out,
                    &format!("rvbench_{}_worker_{key}", self.service),
                    &labels,
                    num,
                );
            }
            if self.service == "manager" {
                for (phase, n) in [
                    ("pending_ack", w.pending_ack),
                    ("acknowledged", w.acknowledged),
                    ("uncertain", w.uncertain),
                ] {
                    sample(
                        &mut out,
                        "rvbench_manager_assignments",
                        &format!("{labels},phase=\"{phase}\""),
                        n as f64,
                    );
                }
            }
        }
        for ((route, method, status), n) in self.http.lock().unwrap().iter() {
            sample(
                &mut out,
                "rvbench_http_requests_total",
                &format!(
                    "{base}{}route=\"{}\",method=\"{method}\",status=\"{status}\"",
                    if base.is_empty() { "" } else { "," },
                    escape(route)
                ),
                *n as f64,
            );
        }
        // TYPE declarations make counter/histogram semantics explicit for all consumers.
        let mut types = BTreeMap::new();
        for line in out.lines() {
            let n = line.split('{').next().unwrap();
            let (name, kind) = if let Some(x) = n.strip_suffix("_bucket") {
                (x, "histogram")
            } else if let Some(x) = n.strip_suffix("_sum") {
                (x, "histogram")
            } else if let Some(x) = n.strip_suffix("_count") {
                (x, "histogram")
            } else {
                (
                    n,
                    if n.ends_with("_total") {
                        "counter"
                    } else {
                        "gauge"
                    },
                )
            };
            types.insert(name, kind);
        }
        let header = types
            .iter()
            .map(|(n, k)| format!("# TYPE {n} {k}\n"))
            .collect::<String>();
        header + &out
    }
    pub fn results(&self, q: ResultQuery) -> Result<ResultsPage, ApiError> {
        validate("ResultQuery", &json!(q)).map_err(ApiError::bad)?;
        let v = self.snapshot();
        let mut rows: Vec<_> = v
            .results
            .iter()
            .filter(|r| {
                q.backend.as_ref().is_none_or(|x| x == &r.backend)
                    && q.kind.as_ref().is_none_or(|x| x == &r.kind)
                    && q.state.as_ref().is_none_or(|x| x == &r.state)
                    && q.op.as_ref().is_none_or(|x| x == &r.op)
                    && q.worker_id.as_ref().is_none_or(|x| x == &r.worker_id)
                    && q.search.as_ref().is_none_or(|x| {
                        format!("{} {} {} {}", r.job_id, r.op, r.worker_id, r.source_sha256)
                            .to_lowercase()
                            .contains(&x.to_lowercase())
                    })
            })
            .collect();
        let total = rows.len();
        let offset = q.offset.unwrap_or(0) as usize;
        let limit = q.limit.unwrap_or(30) as usize;
        let items = rows
            .drain(..)
            .skip(offset)
            .take(limit)
            .cloned()
            .collect::<Vec<_>>();
        Ok(serde_json::from_value(json!({"items":items,"total":total,"offset":offset,"limit":limit,"generated_at":seconds()})).unwrap())
    }
    pub fn leaderboard(&self, q: LeaderboardQuery) -> Result<LeaderboardPage, ApiError> {
        validate("LeaderboardQuery", &json!(q)).map_err(ApiError::bad)?;
        let v = self.snapshot();
        let groups: Vec<_> = v
            .ranking
            .iter()
            .filter(|g| {
                q.backend.as_ref().is_none_or(|b| b == &g.backend)
                    && q.op.as_ref().is_none_or(|o| o == &g.op)
                    && q.group_id.as_ref().is_none_or(|id| id == &g.group_id)
            })
            .collect();
        Ok(serde_json::from_value(json!({"generated_at":seconds(),"groups":groups,"excluded":v.excluded,"policy":"same-board-and-environment; verified-in-run; latest-per-implementation; median-ascending"})).unwrap())
    }
}
pub async fn http_metrics(
    axum::extract::State(c): axum::extract::State<Arc<Console>>,
    req: Request,
    next: Next,
) -> Response {
    let route = req
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_string();
    let method = match req.method().as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "DELETE" => "DELETE",
        _ => "OTHER",
    }
    .to_string();
    let response = next.run(req).await;
    *c.http
        .lock()
        .unwrap()
        .entry((route, method, response.status().as_u16()))
        .or_default() += 1;
    response
}
#[cfg(test)]
mod tests {
    use super::*;
    fn measured(board: &str, source: &str) -> JobRecord {
        let mut req: JobRequest =
            serde_json::from_str(include_str!("../examples/job.json")).unwrap();
        req.kind = JobKind::Performance;
        req.repetitions = 2;
        let mut r = record(req);
        r.board_id = board.into();
        r.state = JobState::Done;
        r.compilation=Some(serde_json::from_value(json!({"status":"passed","details":{"environment":{"runtime":"test","cpus":"0-7"}},"artifacts":[{"path":"candidate.c","sha256":source.repeat(64),"size_bytes":1},{"path":"harness.c","sha256":"f".repeat(64),"size_bytes":1}]})).unwrap());
        r.evaluation=Some(serde_json::from_value(json!({"status":"passed","details":{"correct":true,"samples_ms":[2,4],"reference_samples_ms":[8,12],"clock":"test","compilation_excluded":true},"artifacts":[{"path":"evaluation.json","sha256":"e".repeat(64),"size_bytes":1}]})).unwrap());
        r
    }
    #[test]
    fn rank_requires_real_validation_and_never_mixes_boards_or_environments() {
        let c = Console::new("worker", None);
        let a = measured("A", "a");
        let mut old = a.clone();
        old.job_id = id();
        old.submitted_at = "2000-01-01".into();
        let b = measured("B", "a");
        let mut bad = measured("A", "b");
        bad.evaluation
            .as_mut()
            .unwrap()
            .details
            .additional_properties
            .remove("correct");
        c.update(
            vec![
                (&a, "direct"),
                (&old, "direct"),
                (&b, "direct"),
                (&bad, "direct"),
            ],
            vec![],
            vec![],
            0,
            0.,
        );
        let v = c.snapshot();
        assert_eq!(v.ranking.len(), 2);
        assert_eq!(v.excluded, 1);
        assert!(v.ranking.iter().all(|g| g.entries.len() == 1));
        let row = &v
            .ranking
            .iter()
            .find(|g| g.board_id == "A")
            .unwrap()
            .entries[0];
        assert_eq!(row.job.job_id, a.job_id);
        assert_eq!(row.job.median_ms, Some(3.));
        assert_eq!(row.speedup, Some(10. / 3.));
        for g in &v.ranking {
            validate("RankingGroup", &json!(g)).unwrap();
        }
        assert!(!c.metrics().contains(&a.job_id));
        let before = v.metrics.clone();
        drop(v);
        c.update(
            vec![
                (&a, "direct"),
                (&old, "direct"),
                (&b, "direct"),
                (&bad, "direct"),
            ],
            vec![],
            vec![],
            0,
            0.,
        );
        assert_eq!(before, c.snapshot().metrics);
    }
    #[test]
    fn missing_nonfinite_zero_samples_are_not_rankable() {
        assert!(median(vec![0., 1.]).is_none());
        assert!(median(vec![f64::NAN]).is_none());
        assert!(median(vec![]).is_none());
        assert_eq!(median(vec![1., 4., 2.]), Some(2.));
    }
    #[test]
    fn ranking_separates_runtimes_and_ignores_probe_artifacts() {
        let c = Console::new("worker", None);
        let mut a = measured("A", "a");
        a.request.implementation = Some(KernelImplementation::NativeC);
        a.compilation.as_mut().unwrap().details.additional_properties.insert("environment".into(), json!({
            "implementations":{"native-c":{"ready":true,"probe_compilation":{"random_path":"first","smoke_ms":10}}}
        }));
        let mut b = a.clone();
        b.job_id = id();
        b.submitted_at = "2000-01-01".into();
        b.compilation
            .as_mut()
            .unwrap()
            .details
            .additional_properties
            .get_mut("environment")
            .unwrap()["implementations"]["native-c"]["probe_compilation"] =
            json!({"random_path":"second","smoke_ms":20});
        let mut t = a.clone();
        t.job_id = id();
        t.request.implementation = Some(KernelImplementation::Triton);
        c.update(
            vec![(&a, "direct"), (&b, "direct"), (&t, "direct")],
            vec![],
            vec![],
            0,
            0.,
        );
        let v = c.snapshot();
        assert_eq!(v.ranking.len(), 2);
        assert!(v.ranking.iter().all(|g| g.entries.len() == 1));
        assert!(
            v.ranking
                .iter()
                .all(|g| g.entries[0].job.job_id != b.job_id)
        );
        for group in &v.ranking {
            validate("RankingGroup", &json!(group)).unwrap();
        }
    }
    #[tokio::test]
    async fn projection_publishes_only_after_successful_commit() {
        #[derive(Default, serde::Serialize, serde::Deserialize)]
        struct State {
            n: u32,
        }
        let t = tempfile::tempdir().unwrap();
        let n = Arc::new(std::sync::atomic::AtomicU32::new(99));
        let copy = n.clone();
        let store = Store::<State>::open(&t.path().join("state.sqlite"))
            .unwrap()
            .with_observer(move |s| copy.store(s.n, std::sync::atomic::Ordering::SeqCst))
            .unwrap();
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 0);
        assert!(
            store
                .tx::<()>(|s| {
                    s.n = 7;
                    Err(ApiError::bad("rollback"))
                })
                .await
                .is_err()
        );
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 0);
        store
            .tx(|s| {
                s.n = 3;
                Ok(())
            })
            .await
            .unwrap();
        assert_eq!(n.load(std::sync::atomic::Ordering::SeqCst), 3);
        assert_eq!(store.tx(|s| Ok(s.n)).await.unwrap(), 3);
    }
}
