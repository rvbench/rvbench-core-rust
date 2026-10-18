use rvbench_protocol::*;
use serde_json::json;

#[test]
fn observation_wire_contract_positive_negative_and_legacy() {
    let summary = json!({"schema_version":"1.0.0","generated_at":1.0,"service":"manager","counts":[],"workers":[],"outbox_pending":0,"outbox_oldest_age_seconds":0.0,"collection":{"status":"unavailable","errors":["not configured"]}});
    validate("ObservationSummary", &summary).unwrap();
    let _: ObservationSummary = serde_json::from_value(summary.clone()).unwrap();
    let mut bad = summary;
    bad["outbox_pending"] = json!(-1);
    assert!(validate("ObservationSummary", &bad).is_err());
    let q = json!({"metric_id":"cpu_percent","start":1,"end":2,"step_seconds":30});
    validate("RangeQuery", &q).unwrap();
    let mut bad = q;
    bad["metric_id"] = json!("arbitrary_promql");
    assert!(validate("RangeQuery", &bad).is_err());
    assert!(validate("ResultQuery", &json!({"limit":101})).is_err());
    assert!(validate("MetricPoint", &json!({"timestamp":1})).is_ok());
    assert!(validate("MetricPoint", &json!({"timestamp":1,"value":null})).is_err());
    let mut worker: serde_json::Value =
        serde_json::from_str(include_str!("../examples/worker.json")).unwrap();
    validate("WorkerRegistration", &worker).unwrap();
    worker["observability"] = json!({"metrics_version":"1.0.0","metrics_path":"/metrics","host_metrics_url":"http://127.0.0.1:9100/metrics"});
    validate("WorkerRegistration", &worker).unwrap();
    worker["observability"]["metrics_path"] = json!("/anything");
    assert!(validate("WorkerRegistration", &worker).is_err());
}
