# 并行分片数据暂存与落盘调度服务

在 FastAPI 服务之上提供**并行分片数据暂存（staging）与落盘（flush）调度**能力：
客户端一次提交多个分片，服务端并发处理，保证在**并发冲突、重复提交、进程中断恢复**
等情况下结果一致、可解释。

- 同一目标位置不会被两个任务同时写入；
- 并发饱和、暂存空间不足、目标占用三类拒绝原因明确可区分；
- 按内容指纹去重 + 按分片标识幂等，重复提交不重复占空间；
- 进程重启后已完成分片状态可恢复，落盘前中断的分片可续跑；
- 指纹不符 / 元数据损坏显式报错，绝不静默覆盖；
- 暂存区有容量上限与 LRU 淘汰，被引用数据不淘汰；
- 校验失败或中途异常必须回滚，半成品对后续请求不可见。

## 目录组织

```
<staging_dir>/
├── blobs/                        内容寻址暂存区
│   ├── <digest-hex>              已校验通过的不可变内容块
│   └── .staging-*.tmp            写入中的临时块（失败即删，崩溃重启时清扫）
└── shards/
    └── <shard_id>.json           分片 manifest（原子替换写入）

<output_dir>/                     落盘目标根目录（相对 target_path 拼接于此）
└── <target_path>                 flush 完成后的最终数据（tmp + rename 原子替换）
```

- **manifest** 是每个分片的持久化状态：`shard_id / task_id / target_path / size /
  digest / state / blob_digest / flushed / 时间戳 / error`。写入采用
  `临时文件 + fsync + os.replace`，任何时刻读到的都是完整版本。
- **blob 名就是内容指纹的 hex**，相同内容物理上只存一份；manifest 是 blob 的引用方。

## 指纹与完整性校验规则

- 指纹统一规范为 `algorithm:hex`，支持 `sha256 / sha384 / sha512`（白名单）；
  提交时可省略前缀（裸 hex 按 sha256 解释），非法长度 / 非十六进制立即拒绝。
- 写入时**流式**计算指纹与字节数，并做三项校验：
  1. 实际字节数不得超过声明大小（防止恶意大文件写爆磁盘）；
  2. 写完比对总大小；
  3. 比对实际指纹与客户端声明指纹。
  任一不通过：删除临时块、释放容量预留、分片置 `failed`，错误码
  `checksum_mismatch`，并区分 `size_exceeds_declared / size_mismatch /
  digest_mismatch` 三个 reason。
- 中断恢复续跑落盘前，会对已暂存 blob **重新完整校验**（`verify_blob`）；
  blob 被篡改时 reason=`blob_corrupted`，拒绝落盘。
- manifest 损坏（非法 JSON / 字段缺失 / 类型损坏 / 未知状态）在加载与启动扫描时
  抛 `metadata_corrupted`，**绝不静默忽略或覆盖旧数据**。

## 状态机

```
PENDING -> STAGING -> STAGED -> FLUSHING -> COMPLETED
              |          |          |
              v          v          v
           FAILED    CANCELED     FAILED
```

- `COMPLETED + flushed=true`：数据已在最终位置，blob 退化为可淘汰缓存（解除引用）；
- `COMPLETED + flushed=false`：仅暂存模式（`STAGING_FLUSH_ON_COMMIT=false`），
  blob 保持引用、禁止淘汰；
- 崩溃恢复规则：
  - `STAGED / FLUSHING` 且 blob 还在 → 重新 pin，回到 `STAGED`，重提只跑落盘；
  - `COMPLETED + flushed=true` → 保持完成，幂等命中；
  - `STAGING / FAILED / CANCELED` → 无有效 blob，重新提交全量重跑。

## 并发策略

- **目标互斥**：每个规范化目标路径一把锁。提交时对本批目标排序后依次非阻塞抢占，
  任一目标已被其他任务持有 → 整批 `409 target_locked/reason=target_busy`，
  错误体带 `held_by` 任务标识。
- **并发上限**：大小为 `STAGING_MAX_CONCURRENCY` 的工作槽信号量。仅当系统饱和
  （槽全被占）时拒绝新任务 `503 concurrency_limit/reason=concurrency_slots_busy`；
  未饱和则接收，批次内分片在系统内部排队，因此“3 个分片 / 并发度 2”可以正常跑。
- **批内自检**：同批分片标识重复 → `400 duplicate_shard_id_in_batch`；
  同批目标位置重复 → `400 duplicate_target_in_batch`。
- 每个分片一个工作线程；任务可取消（`POST /tasks/{id}/cancel`），取消点覆盖
  排队中 / 暂存中 / 落盘前，已暂存未落盘的取消会解除 blob 引用。

## 容量与淘汰策略

- 容量账本 = 已落盘 blob 字节 + **在途预留字节**（边写边算，防止超卖）。
- 提交时先做容量准入预估：`已占用 + 新需求 - 可回收 > 容量` 则整批拒绝
  `507 capacity_exceeded/reason=admission_capacity`；单片超过总容量时
  BlobStore 还可能给出 `shard_larger_than_capacity`；写入时空间仍不够且无可淘汰
  数据则 `no_reclaimable_space`。
- **淘汰策略为 LRU**：仅淘汰引用计数为 0 的 blob；`STAGING/STAGED/FLUSHING`
  与“仅暂存”的完成分片都持有引用，**引用中的数据永不淘汰**。
- 淘汰可通过 `STAGING_EVICTION_ENABLED=false` 关闭，关闭后空间不足为硬拒绝，
  不删除任何旧 blob。

## 去重与幂等

- **内容去重**：blob 按指纹命名，不同 shard_id 相同内容共享一个 blob、一份空间。
- **分片幂等**：以 `shard_id` 为幂等键查历史 manifest：
  - 指纹 / 目标 / 大小完全一致且已完成 → 直接返回完成（`idempotent_hit`），不重写；
  - 已完成但参数不一致 → `400 invalid_request/reason=idempotent_conflict`，拒绝覆盖；
  - `STAGED` 状态 → 只续跑落盘（`flush_only`）。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/health` | 健康检查、容量 / 已用 / 可回收字节 |
| `POST` | `/tasks` | 提交任务（`202`），body 见下 |
| `GET`  | `/tasks/{task_id}` | 进度：按状态计数 + 每个分片明细 |
| `POST` | `/tasks/{task_id}/cancel` | 请求取消 |
| `GET`  | `/shards/{shard_id}` | 分片持久化状态（重启后仍可查，`404`） |

提交 body（数据以 base64 内联，便于无外部依赖地端到端使用）：

```json
{
  "shards": [
    {
      "shard_id": "s-1",
      "target_path": "2026/q3/a.bin",
      "digest": "sha256:<hex>",
      "data_base64": "<base64>"
    }
  ]
}
```

错误响应统一结构：

```json
{"error": "target_locked", "reason": "target_busy", "message": "...", "context": {"held_by": "..."}}
```

| HTTP | error | 典型 reason |
| --- | --- | --- |
| 400 | `invalid_request` | `empty_batch / too_many_shards / duplicate_shard_id_in_batch / duplicate_target_in_batch / idempotent_conflict / bad_base64` |
| 404 | `task_not_found` / `shard_not_found` | — |
| 409 | `target_locked` / `metadata_corrupted` | `target_busy / manifest_invalid_json / manifest_bad_fields / ...` |
| 422 | `checksum_mismatch` | `digest_mismatch / size_mismatch / size_exceeds_declared / blob_corrupted` |
| 503 | `concurrency_limit` | `concurrency_slots_busy` |
| 507 | `capacity_exceeded` | `admission_capacity / shard_larger_than_capacity / no_reclaimable_space` |

## 配置项（环境变量）

| 变量 | 默认值 | 含义 |
| --- | --- | --- |
| `STAGING_DIR` | 系统临时目录 `/shard-staging` | 暂存根目录（blobs + shards） |
| `OUTPUT_DIR` | 空（target_path 按绝对路径解释） | 落盘根目录；设置后相对 target 拼接到此 |
| `STAGING_CAPACITY_BYTES` | `67108864`（64 MiB） | 暂存区容量上限 |
| `STAGING_MAX_CONCURRENCY` | `4` | 同时执行的分片数上限（工作槽） |
| `STAGING_MAX_SHARDS_PER_TASK` | `256` | 单任务最大分片数 |
| `STAGING_DIGEST_ALGORITHM` | `sha256` | 默认指纹算法（白名单内） |
| `STAGING_EVICTION_ENABLED` | `true` | 是否允许 LRU 淘汰无引用 blob |
| `STAGING_FLUSH_ON_COMMIT` | `true` | 暂存校验通过后是否立即落盘 |
| `STAGING_REQUEST_TIMEOUT_SECONDS` | `30` | 请求超时预留配置 |
| `HOST` / `PORT` | `127.0.0.1` / `8000` | 监听地址 |

## 运行与测试

```bash
uv sync --all-groups
uv run python main.py                     # 启动服务
STAGING_MAX_CONCURRENCY=2 STAGING_CAPACITY_BYTES=1048576 \
  OUTPUT_DIR=./data-out uv run python main.py

uv run pytest                              # 全部单测
uv run pytest -k concurrency               # 按场景筛选
```

测试全部使用 `pytest` 临时目录（`tmp_path`）与可控假数据源
（`BlockingSource` 阻塞闸门 / `FailingSource` 中途异常），**不依赖任何外部服务**，
覆盖：

- `tests/test_concurrency.py`：目标互斥、并发上限 503、批内重复、进度、取消；
- `tests/test_dedup.py`：内容去重、幂等命中、参数冲突拒绝覆盖；
- `tests/test_recovery.py`：重启恢复、残留 tmp 清扫、落盘前中断续跑、manifest 损坏；
- `tests/test_eviction.py`：LRU 淘汰、引用保护、容量拒绝原因区分、在途计量、关淘汰；
- `tests/test_checksum.py`：指纹 / 大小不符、中途异常、blob 篡改、回滚干净；
- `tests/test_api.py`：HTTP 提交 / 进度 / 取消 / 409·503·507·422·400 错误码区分。

用例通过 caplog 将日志级别提到 INFO，日志中统一带 `task_id`、`shard_id`
与判定依据（`decision=idempotent_hit/full/flush_only`、`reason=...`、
`state=...` 等），便于审计为什么某个分片被接受、拒绝、淘汰或回滚。
