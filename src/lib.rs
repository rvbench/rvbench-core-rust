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
/// Shared by direct admission and the manager's push/pull scheduler. Older workers
/// only support their original implicit runtime; never infer cross-runtime support.
pub fn worker_supports_request(worker: &WorkerRegistration, request: &JobRequest) -> bool {
    if worker.backend != request.backend || !worker.kinds.contains(&request.kind) {
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
