<p align="center">
  <img src="assets/rvbench-logo.svg" alt="rvbench" width="720">
</p>

# rvbench-protocol

rvbench 的共享 wire 合同 crate。它是跨服务边界的唯一事实来源，负责：

- **OpenAPI 副本**：`openapi.json` 是主仓 [`rvbench/contracts/openapi.json`](https://github.com/rvbench/rvbench/blob/main/contracts/openapi.json) 的发布副本，定义全部 wire 模型，`SPEC` 常量在编译期嵌入。
- **生成类型**：`generated/types.rs` 由 `openapi-to-rust` 从 `openapi.json` 生成，禁止手改。
- **JSON Schema 校验**：`validate` / `decode` 按 schema 名称校验任意 JSON 值。
- **canonical JSON / SHA-256**：`json` 模块提供稳定序列化与摘要（`digest`、`spec_digest`）。
- **事务存储**：`Store<T>` 基于 SQLite WAL 提供单快照原子事务与提交后观测。

配套文档：`invariants.md`（一致性与不变量）、`observability.md` 与 `metrics.json`（观测合同）、`examples/`（正反例）、`codegen.toml`（生成配置）。

## 变更流程

正本在主仓 `rvbench` 的 `contracts/openapi.json`；本仓只消费，不发起变更。

```sh
# 主仓
$EDITOR contracts/openapi.json
python tools/sync_contract.py          # 单向同步到本仓 openapi.json

# 本仓
openapi-to-rust generate -c codegen.toml
cargo test
```

> 生成物禁止手改；本仓 `openapi.json` 同样不手改——它是副本，任何 wire 变更都必须先落到主仓正本。
