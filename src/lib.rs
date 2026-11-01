//! Shared contract validation and transactional persistence. Wire types are generated.
#[path = "../generated/types.rs"]
mod wire;
use anyhow::{Result, ensure};
use axum::{
    Json,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{
    path::Path,
    sync::{Arc, Mutex, OnceLock},
};
pub use wire::*;
pub mod console;
pub mod json;
pub use json::{load_json, parse_json, spec_digest};

pub const SPEC: &str = include_str!("../openapi.json");
/// Minimal valid JobRequest used by manager / worker tests and tooling.
pub const EXAMPLE_JOB: &str = include_str!("../examples/job.json");
/// Self-contained four-tab console served by manager and worker at `/ui`.
pub const CONSOLE_HTML: &str = include_str!("../webui/console.html");
pub fn validate(name: &str, value: &Value) -> Result<()> {
    static VALIDATORS: OnceLock<std::collections::HashMap<String, jsonschema::Validator>> =
        OnceLock::new();
    let validators = VALIDATORS.get_or_init(|| {
        let spec: Value = serde_json::from_str(SPEC).unwrap();
        spec["components"]["schemas"].as_object().unwrap().keys().map(|name| {
            let schema = json!({"$ref":format!("#/components/schemas/{name}"), "components":spec["components"]});
            (name.clone(), jsonschema::options().should_validate_formats(true).build(&schema).expect("checked contract"))
        }).collect()
    });
    let validator = validators
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("unknown schema"))?;
    validator
        .validate(value)
        .map_err(|e| anyhow::anyhow!("{name}: {e}"))?;
    Ok(())
}
pub fn decode<T: DeserializeOwned>(name: &str, value: Value) -> Result<T, ApiError> {
    validate(name, &value).map_err(ApiError::bad)?;
    serde_json::from_value(value).map_err(ApiError::bad)
}
pub fn digest(value: &impl Serialize) -> String {
    crate::json::spec_digest(&serde_json::to_value(value).unwrap())
}
/// Parse the original body before schema validation; serde_json::Value alone would
/// silently keep the last value of a duplicate object key.
pub struct RawJson(pub Value);
impl<S: Send + Sync> axum::extract::FromRequest<S> for RawJson {
    type Rejection = ApiError;
    async fn from_request(
        request: axum::extract::Request,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        let bytes = axum::body::Bytes::from_request(request, state)
            .await
            .map_err(|e| ApiError(e.status(), e.to_string()))?;
        let text = std::str::from_utf8(&bytes).map_err(ApiError::bad)?;
        Ok(Self(crate::json::parse_json(text).map_err(ApiError::bad)?))
    }
}
pub fn id() -> String {
    uuid::Uuid::new_v4().to_string()
}
pub fn now() -> String {
    chrono::Utc::now().to_rfc3339()
}
pub fn seconds() -> f64 {
    chrono::Utc::now().timestamp_millis() as f64 / 1000.0
}
pub fn terminal(s: &JobState) -> bool {
    !matches!(s, JobState::Queued | JobState::Running)
}
pub fn record(request: JobRequest) -> JobRecord {
    JobRecord {
        phase_timings_seconds: None,
        job_id: id(),
        request,
        state: JobState::Queued,
        worker_id: String::new(),
        board_id: String::new(),
        instance_id: String::new(),
        submitted_at: now(),
        started_at: None,
        finished_at: None,
        compilation: None,
        evaluation: None,
        error: None,
        logs: String::new(),
        assignment_id: None,
        attempt_id: None,
        lease_id: None,
    }
}
#[derive(Debug)]
pub struct ApiError(pub StatusCode, pub String);
impl ApiError {
    pub fn bad(e: impl std::fmt::Display) -> Self {
        Self(StatusCode::BAD_REQUEST, e.to_string())
    }
    pub fn conflict(e: impl std::fmt::Display) -> Self {
        Self(StatusCode::CONFLICT, e.to_string())
    }
    pub fn missing() -> Self {
        Self(StatusCode::NOT_FOUND, "not found".into())
    }
}
impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        Self(StatusCode::INTERNAL_SERVER_ERROR, e.to_string())
    }
}
impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error":self.1}))).into_response()
    }
}

/// One atomic control-state snapshot per service. SQLite WAL + FULL sync; no process/network IO
/// inside transactions. Blocking database work always runs off the Tokio executor.
type StoreObserver<T> = Arc<dyn Fn(&T) + Send + Sync>;
pub struct Store<T> {
    connection: Arc<Mutex<rusqlite::Connection>>,
    marker: std::marker::PhantomData<T>,
    observer: Option<StoreObserver<T>>,
}
impl<T> Clone for Store<T> {
    fn clone(&self) -> Self {
        Self {
            connection: self.connection.clone(),
            marker: std::marker::PhantomData,
            observer: self.observer.clone(),
        }
    }
}
impl<T: Default + Serialize + DeserializeOwned + Send + 'static> Store<T> {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let c = rusqlite::Connection::open(path)?;
        c.busy_timeout(std::time::Duration::from_secs(5))?;
        c.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; CREATE TABLE IF NOT EXISTS control_state (id INTEGER PRIMARY KEY CHECK(id=1), body TEXT NOT NULL);")?;
        c.execute(
            "INSERT OR IGNORE INTO control_state VALUES(1,?1)",
            [serde_json::to_string(&T::default())?],
        )?;
        Ok(Self {
            connection: Arc::new(Mutex::new(c)),
            marker: std::marker::PhantomData,
            observer: None,
        })
    }
    /// Publish an initial projection and then only committed mutations, in commit order.
    pub fn with_observer(mut self, observer: impl Fn(&T) + Send + Sync + 'static) -> Result<Self> {
        let body: String = self.connection.lock().unwrap().query_row(
            "SELECT body FROM control_state WHERE id=1",
            [],
            |r| r.get(0),
        )?;
        observer(&serde_json::from_str(&body)?);
        self.observer = Some(Arc::new(observer));
        Ok(self)
    }
    pub async fn tx<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut T) -> Result<R, ApiError> + Send + 'static,
    ) -> Result<R, ApiError> {
        let connection = self.connection.clone();
        let observer = self.observer.clone();
        tokio::task::spawn_blocking(move || {
            let mut c = connection
                .lock()
                .map_err(|_| anyhow::anyhow!("database poisoned"))?;
            let tx = c.transaction().map_err(anyhow::Error::from)?;
            let body: String = tx
                .query_row("SELECT body FROM control_state WHERE id=1", [], |r| {
                    r.get(0)
                })
                .map_err(anyhow::Error::from)?;
            let mut state: T = serde_json::from_str(&body).map_err(anyhow::Error::from)?;
            let result = f(&mut state)?;
            let next = serde_json::to_string(&state).map_err(anyhow::Error::from)?;
            let changed = next != body;
            if changed {
                tx.execute("UPDATE control_state SET body=?1 WHERE id=1", [next])
                    .map_err(anyhow::Error::from)?;
            }
            tx.commit().map_err(anyhow::Error::from)?;
            if changed && let Some(observer) = observer {
                observer(&state);
            }
            Ok(result)
        })
        .await
        .map_err(anyhow::Error::from)?
    }
}
pub fn validate_request(request: &JobRequest) -> Result<()> {
    validate("JobRequest", &serde_json::to_value(request)?)?;
    ensure!(
        serde_json::to_vec(request)?.len() <= 2 * 1024 * 1024,
        "request exceeds 2 MiB UTF-8"
    );
    ensure!(
        request
            .source
            .as_ref()
            .is_none_or(|s| s.len() <= 1024 * 1024),
        "source exceeds 1 MiB UTF-8"
    );
    let size = request
        .shape
        .iter()
        .try_fold(1i64, |n, x| n.checked_mul(*x));
    ensure!(
        size.is_some_and(|n| n <= 16_777_216),
        "total tensor elements exceed 16777216"
    );
    Ok(())
}

pub fn default_implementation(backend: &BackendId) -> KernelImplementation {
    match backend {
        BackendId::K1 => KernelImplementation::NativeC,
        BackendId::K3 => KernelImplementation::Triton,
    }
}
pub fn implementation(request: &JobRequest) -> KernelImplementation {
    request
        .implementation
        .clone()
        .unwrap_or_else(|| default_implementation(&request.backend))
}
/// Runtime ids are stable strings; the legacy enum maps onto them one way only.
pub const RUNTIME_EP: &str = "spacemit-ep";
pub const RUNTIME_TRITON: &str = "spacemit-triton";
pub fn runtime_of_implementation(implementation: &KernelImplementation) -> &'static str {
    match implementation {
        KernelImplementation::NativeC => RUNTIME_EP,
        KernelImplementation::Triton => RUNTIME_TRITON,
    }
}
pub fn default_runtime(backend: &BackendId) -> &'static str {
    runtime_of_implementation(&default_implementation(backend))
}
/// Split `id@major.minor[.patch]` into its halves. The version half is a pin,
/// not part of the runtime identity.
pub fn split_runtime_ref(value: &str) -> (&str, Option<&str>) {
    match value.split_once('@') {
        Some((id, version)) => (id, Some(version)),
        None => (value, None),
    }
}
pub fn version_matches(declared: &str, wanted: Option<&str>) -> bool {
    match wanted {
        None => true,
        Some(want) if want.matches('.').count() == 2 => declared == want,
        Some(want) => declared == want || declared.starts_with(&format!("{want}.")),
    }
}
/// The runtime a request asks for, before any worker is consulted.
pub fn runtime_ref(request: &JobRequest) -> String {
    request
        .runtime
        .clone()
        .unwrap_or_else(|| runtime_of_implementation(&implementation(request)).to_string())
}
/// A source kind is optional on the wire; it falls back to the legacy
/// implementation and finally to the runtime's first declared kind.
pub fn source_kind_of(request: &JobRequest, runtime: Option<&RuntimeDecl>) -> Option<SourceKind> {
    request.source_kind.clone().or_else(|| {
        request
            .implementation
            .as_ref()
            .map(|implementation| match implementation {
                KernelImplementation::NativeC => SourceKind::C,
                KernelImplementation::Triton => SourceKind::Python,
            })
    })
    .or_else(|| runtime.and_then(|r| r.source_kinds.first().cloned()))
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Placement {
    pub runtime: String,
    pub version: String,
    pub context: Option<String>,
}
/// Admission over declarations only: no probing, no inference beyond the
/// documented legacy mapping. Returns the slot the request lands in.
pub fn admit(worker: &WorkerRegistration, request: &JobRequest) -> Result<Placement, RejectReason> {
    if worker.backend != request.backend {
        return Err(RejectReason::BackendMismatch);
    }
    if !worker.kinds.contains(&request.kind) {
        return Err(RejectReason::KindUnsupported);
    }
    let runtimes = worker
        .runtimes
        .as_ref()
        .filter(|runtimes| !runtimes.is_empty())
        .ok_or(RejectReason::RuntimeNotInstalled)?;
    let selected = runtime_ref(request);
    let (id, wanted) = split_runtime_ref(&selected);
    if !runtimes.iter().any(|r| r.id == id) {
        return Err(RejectReason::RuntimeNotInstalled);
    }
    let Some(declared) = runtimes
        .iter()
        .find(|r| r.id == id && version_matches(&r.version, wanted))
    else {
        return Err(RejectReason::VersionPinnedMissing);
    };
    if !declared.ready {
        return Err(RejectReason::RuntimeNotReady);
    }
    if let Some(kind) = source_kind_of(request, Some(declared)) {
        if !declared.source_kinds.contains(&kind) {
            return Err(RejectReason::SourceKindUnsupported);
        }
    }
    if !declared.ops.contains(&request.op) {
        return Err(RejectReason::OpUnsupported);
    }
    if !declared
        .dtypes
        .iter()
        .any(|dtype| dtype.as_str() == request.dtype.as_str())
    {
        return Err(RejectReason::DtypeUnsupported);
    }
    let context = worker.slots.as_ref().and_then(|slots| {
        slots
            .iter()
            .find(|slot| slot.runtime == declared.id)
            .map(|slot| slot.context.clone())
    });
    Ok(Placement {
        runtime: declared.id.clone(),
        version: declared.version.clone(),
        context,
    })
}
/// Shared by direct admission and the manager's push/pull scheduler. Older workers
/// only support their original implicit runtime; never infer cross-runtime support.
pub fn worker_supports_request(worker: &WorkerRegistration, request: &JobRequest) -> bool {
    if worker
        .runtimes
        .as_ref()
        .is_some_and(|runtimes| !runtimes.is_empty())
    {
        return admit(worker, request).is_ok();
    }
    legacy_worker_supports_request(worker, request)
}
fn legacy_worker_supports_request(worker: &WorkerRegistration, request: &JobRequest) -> bool {
    if worker.backend != request.backend || !worker.kinds.contains(&request.kind) {
        return false;
    }
    // A legacy worker serves exactly one implicit runtime; an explicit request for
    // another id must not fall through to it.
    if runtime_ref(request) != runtime_of_implementation(&implementation(request)) {
        return false;
    }
    let env = &worker.environment.additional_properties;
    let selected = implementation(request);
    let (ops, dtypes) = if let Some(modes) = env.get("implementations") {
        let Some(capability) = modes.get(selected.as_str()) else {
            return false;
        };
        if capability["ready"] != true {
            return false;
        }
        (capability.get("ops"), capability.get("dtypes"))
    } else {
        // Older HTTP contracts reject the new field, even when it names their
        // implicit runtime. Keep its request/digest intact and wait for an upgrade.
        if request.implementation.is_some() {
            return false;
        }
        if selected != default_implementation(&worker.backend) {
            return false;
        }
        (env.get("ops"), env.get("dtypes"))
    };
    ops.and_then(Value::as_array)
        .is_some_and(|ops| ops.contains(&json!(request.op)))
        && dtypes
            .and_then(Value::as_array)
            .is_some_and(|ds| ds.contains(&json!(request.dtype)))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn contract_examples_and_unknown_fields() {
        let good: Value = serde_json::from_str(include_str!("../examples/job.json")).unwrap();
        validate("JobRequest", &good).unwrap();
        assert!(
            validate(
                "JobRequest",
                &serde_json::from_str(include_str!("../examples/invalid-job.json")).unwrap()
            )
            .is_err()
        );
        let mut unknown = good.clone();
        unknown["token"] = json!("removed");
        assert!(validate("JobRequest", &unknown).is_err());
        let dto: JobRequest = serde_json::from_value(good.clone()).unwrap();
        assert_eq!(serde_json::to_value(dto).unwrap(), good);
    }
    #[test]
    fn canonical_digest_is_order_independent() {
        assert_eq!(
            digest(&json!({"a":1,"b":{"x":2,"y":3}})),
            digest(&json!({"b":{"y":3,"x":2},"a":1}))
        );
        assert_eq!(
            digest(&json!({"small":1e-7})),
            crate::json::spec_digest(&json!({"small":1e-7}))
        );
    }
    #[test]
    fn implementation_is_independent_of_hardware_and_optional() {
        for example in [
            include_str!("../examples/k3-native-c.json"),
            include_str!("../examples/k1-triton.json"),
        ] {
            let value: Value = serde_json::from_str(example).unwrap();
            validate("JobRequest", &value).unwrap();
            let request: JobRequest = serde_json::from_value(value.clone()).unwrap();
            assert_eq!(serde_json::to_value(request).unwrap(), value);
        }
        let mut value: Value = serde_json::from_str(include_str!("../examples/job.json")).unwrap();
        for invalid in [json!("cuda"), json!(null), json!(123)] {
            value["implementation"] = invalid;
            assert!(validate("JobRequest", &value).is_err());
        }
    }
    #[test]
    fn capabilities_match_runtime_op_and_dtype_without_inference() {
        for hardware in [BackendId::K1, BackendId::K3] {
            let mut request: JobRequest =
                serde_json::from_str(include_str!("../examples/job.json")).unwrap();
            request.backend = hardware.clone();
            let mut worker: WorkerRegistration =
                serde_json::from_str(include_str!("../examples/worker.json")).unwrap();
            worker.backend = hardware.clone();
            worker.kinds = vec![request.kind.clone()];
            worker.environment =
                serde_json::from_value(json!({"ops":["add"],"dtypes":["float32"]})).unwrap();
            assert_eq!(implementation(&request), default_implementation(&hardware));
            assert!(worker_supports_request(&worker, &request));
            // Old workers cannot decode the new optional request field.
            request.implementation = Some(default_implementation(&hardware));
            assert!(!worker_supports_request(&worker, &request));
            worker.environment = serde_json::from_value(
                json!({"ops":["add"],"dtypes":["float32"],"implementations":{
                    "native-c":{"ready":true,"ops":["add","abs"],"dtypes":["float32"]},
                    "triton":{"ready":false,"ops":["add","sum"],"dtypes":["float32","float16"]}
                }}),
            )
            .unwrap();
            request.implementation = Some(KernelImplementation::NativeC);
            assert!(worker_supports_request(&worker, &request));
            request.dtype = JobRequestDtype::Float16;
            assert!(!worker_supports_request(&worker, &request));
            request.implementation = Some(KernelImplementation::Triton);
            assert!(!worker_supports_request(&worker, &request));
            worker
                .environment
                .additional_properties
                .get_mut("implementations")
                .unwrap()["triton"]["ready"] = json!(true);
            assert!(worker_supports_request(&worker, &request));
            request.op = "abs".into();
            assert!(!worker_supports_request(&worker, &request));
            worker
                .environment
                .additional_properties
                .get_mut("implementations")
                .unwrap()
                .as_object_mut()
                .unwrap()
                .remove("triton");
            assert!(!worker_supports_request(&worker, &request));
        }
    }
    #[test]
    fn identifiers_and_role_references_are_checked() {
        let mut ack = serde_json::json!({"worker_id":"worker","instance_id":id(),"lease_id":id(),"worker_job_id":id()});
        validate("Ack", &ack).unwrap();
        ack["instance_id"] = json!("not-a-uuid");
        assert!(validate("Ack", &ack).is_err());
        let spec: Value = serde_json::from_str(SPEC).unwrap();
        let schemas = &spec["components"]["schemas"];
        fn refs(value: &Value, schemas: &Value, seen: &mut usize) {
            match value {
                Value::Object(map) => {
                    if let Some(Value::String(r)) = map.get("$ref") {
                        let name = r
                            .strip_prefix("#/components/schemas/")
                            .unwrap_or_else(|| panic!("non-schema ref {r}"));
                        assert!(schemas.get(name).is_some(), "unresolved {r}");
                        *seen += 1;
                    }
                    for v in map.values() {
                        refs(v, schemas, seen);
                    }
                }
                Value::Array(items) => {
                    for v in items {
                        refs(v, schemas, seen);
                    }
                }
                _ => {}
            }
        }
        let mut seen = 0;
        refs(&spec, schemas, &mut seen);
        assert!(seen > 0, "spec declares no references");
    }
}

#[cfg(test)]
mod runtime_admission {
    use super::*;
    use serde_json::{Value, json};

    fn worker(runtimes: Value, slots: Value) -> WorkerRegistration {
        serde_json::from_value(json!({
            "worker_id": "k3-1",
            "board_id": "K3-1-32G",
            "instance_id": "00000000-0000-0000-0000-000000000001",
            "backend": "k3",
            "kinds": ["func", "correctness", "performance"],
            "delivery": "pull",
            "url": "http://127.0.0.1:18801",
            "capacity": 1,
            "direct_active": 0,
            "ready": true,
            "environment": {},
            "protocol_version": "1.0.0",
            "contexts": [{"id": "x100", "cpus": "0-7"}, {"id": "a100", "cpus": "8-15", "ai_thread": true}],
            "runtimes": runtimes,
            "slots": slots
        }))
        .unwrap()
    }
    fn decl(id: &str, version: &str, kinds: Value) -> Value {
        json!({
            "id": id,
            "version": version,
            "source_kinds": kinds,
            "entry": {"symbol": "candidate", "abi": "void f(const float*,const float*,float*,int)"},
            "ops": ["add"],
            "dtypes": ["float32"],
            "ready": true,
            "default": true
        })
    }
    fn request(runtime: Option<&str>, source_kind: Option<&str>) -> JobRequest {
        let mut request: JobRequest = serde_json::from_str(EXAMPLE_JOB).unwrap();
        request.runtime = runtime.map(str::to_string);
        request.source_kind = source_kind.map(|kind| match kind {
            "c" => SourceKind::C,
            "python" => SourceKind::Python,
            "triton" => SourceKind::Triton,
            _ => SourceKind::Mlir,
        });
        request
    }

    #[test]
    fn legacy_implementation_maps_to_runtime_id() {
        assert_eq!(runtime_ref(&request(None, None)), RUNTIME_TRITON);
        let mut native = request(None, None);
        native.implementation = Some(KernelImplementation::NativeC);
        assert_eq!(runtime_ref(&native), RUNTIME_EP);
        assert_eq!(split_runtime_ref("spacemit-triton@0.3"), ("spacemit-triton", Some("0.3")));
        assert!(version_matches("0.3.1", Some("0.3")));
        assert!(version_matches("0.3.1", Some("0.3.1")));
        assert!(!version_matches("0.3.1", Some("0.4")));
    }

    #[test]
    fn admission_resolves_runtime_and_slot() {
        let worker = worker(
            json!([decl("spacemit-ep", "0.2.0", json!(["c"])), decl("spacemit-triton", "0.3.1", json!(["python", "triton"]))]),
            json!([{"context": "x100", "runtime": "spacemit-ep", "capacity": 1}, {"context": "a100", "runtime": "spacemit-triton", "capacity": 1}]),
        );
        let placement = admit(&worker, &request(Some("spacemit-triton@0.3"), None)).unwrap();
        assert_eq!(placement.runtime, "spacemit-triton");
        assert_eq!(placement.version, "0.3.1");
        assert_eq!(placement.context.as_deref(), Some("a100"));
    }

    #[test]
    fn admission_rejections_are_specific() {
        let worker = worker(
            json!([decl("spacemit-ep", "0.2.0", json!(["c"]))]),
            json!([]),
        );
        assert_eq!(
            admit(&worker, &request(Some("spacemit-triton"), None)).unwrap_err(),
            RejectReason::RuntimeNotInstalled
        );
        assert_eq!(
            admit(&worker, &request(Some("spacemit-ep@9.9"), None)).unwrap_err(),
            RejectReason::VersionPinnedMissing
        );
        assert_eq!(
            admit(&worker, &request(Some("spacemit-ep"), Some("python"))).unwrap_err(),
            RejectReason::SourceKindUnsupported
        );
        let mut unsupported = request(Some("spacemit-ep"), None);
        unsupported.dtype = JobRequestDtype::Float16;
        assert_eq!(
            admit(&worker, &unsupported).unwrap_err(),
            RejectReason::DtypeUnsupported
        );
        let mut wrong_board = request(Some("spacemit-ep"), None);
        wrong_board.backend = BackendId::K1;
        assert_eq!(
            admit(&worker, &wrong_board).unwrap_err(),
            RejectReason::BackendMismatch
        );
    }

    #[test]
    fn workers_without_runtimes_keep_the_legacy_path() {
        let legacy = worker(Value::Null, Value::Null);
        let mut legacy = serde_json::to_value(&legacy).unwrap();
        legacy.as_object_mut().unwrap().remove("runtimes");
        legacy.as_object_mut().unwrap().remove("slots");
        legacy["environment"] = json!({"ops": ["add"], "dtypes": ["float32"]});
        let legacy: WorkerRegistration = serde_json::from_value(legacy).unwrap();
        let mut request = request(None, None);
        request.implementation = None;
        assert!(worker_supports_request(&legacy, &request));
        request.runtime = Some("spacemit-ep".into());
        assert!(!worker_supports_request(&legacy, &request));
    }
}
