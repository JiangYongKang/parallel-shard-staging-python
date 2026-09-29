# parallel-shard-staging

在 FastAPI HTTP 服务之上提供**并行分片数据暂存与落盘调度**：客户端一次提交多个分片，
服务端并发落盘，在并发冲突、重复提交、中断恢复场景下给出一致、可解释的结果。

## 运行与测试

```bash
uv sync
uv run python main.py        # 启动 HTTP 服务
uv run pytest                # 运行测试（日志含任务/分片标识与判定依据）
```

## 暂存区组织方式

暂存区是一个本地目录（默认 `./staging-data`）：

```
<root>/blobs/<fingerprint>        已提交的完整数据，按内容指纹命名，天然去重
<root>/tmp/<task>-<shard>.part    写入中的临时文件，提交前对外不可见
<root>/index.json                 元数据：指纹 -> {size, refs, last_used}
```

- 写入先落到 `tmp/*.part`，**边写边算 sha256**，校验通过后 `os.replace` 原子改名到
  `blobs/`，因此任何时刻读到的要么是完整数据，要么没有数据。
- `index.json` 通过"写临时文件 + 原子改名"持久化；进程重启时重新加载，
  已完成分片的状态（指纹、大小、引用计数）随之恢复。
- 启动时会清理 `tmp/` 下崩溃残留的 `.part` 半成品，绝不让半成品被后续请求读到。
- `index.json` 损坏（JSON 非法、字段缺失、与磁盘 blob 大小不一致）时抛出
  `MetadataCorruptedError` 并拒绝启动，**不静默覆盖旧数据**。

## 指纹与校验规则

- 指纹为内容的 **sha256 十六进制**，由客户端在提交时声明，同时声明字节数。
- 提交时服务端流式复算指纹与字节数，二者任一不符即抛 `ChecksumMismatchError`：
  临时文件删除、任务标记失败，已有旧数据不受影响。
- 指纹已存在于 `blobs/` 时直接**去重命中**（结果状态 `deduped`），不重复占用空间，
  重复提交因此是幂等的；去重命中的分片也不计入容量预检。

## 并发与淘汰策略

- 一次 `POST /tasks` 提交的一批分片构成一个任务，由线程池并发落盘。
- **同一 target 同一时刻只允许一个任务写入**：后到分片立即以 `target_busy` 拒绝，
  不排队等待，避免相互覆盖。
- 在途任务数超过 `max_workers + max_pending` 时，提交以 `concurrency_limit` 拒绝
  （HTTP 429）。
- 提交前做容量预检：新指纹的总字节数超过可用空间时以 `capacity_exceeded` 拒绝
  （HTTP 507）。可用空间 = 剩余空间 + 可淘汰 blob 总大小。
- 空间不足时按 **LRU 淘汰** `refs == 0` 的 blob；`refs > 0`（正被引用）的数据
  **不会被淘汰**，若淘汰后仍放不下则报容量错误。
- 任务可查询进度（`GET /tasks/{id}`：状态、完成数、每个分片的判定依据），
  也可取消（`POST /tasks/{id}/cancel`）：未开始的分片标记 `cancelled`，
  进行中的分片随校验/回滚逻辑清理。

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/tasks` | 提交一批分片（`shard_id`/`target`/`fingerprint`/`size`/`content_b64`），返回 `task_id`；拒绝时 429（并发）或 507（容量） |
| GET | `/tasks/{id}` | 查询任务进度与每个分片的状态、判定依据 |
| POST | `/tasks/{id}/cancel` | 取消任务 |
| GET | `/stats` | 暂存区用量（`used_bytes` / `max_bytes`） |

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `STAGING_DIR` | `./staging-data` | 暂存区根目录 |
| `STAGING_MAX_BYTES` | `268435456`（256MiB） | 暂存区容量上限 |
| `STAGING_MAX_WORKERS` | `4` | 并发落盘线程数 |
| `STAGING_MAX_PENDING` | `8` | 允许排队的任务数 |

## 代码结构

- `staging/models.py` — 分片/任务类型、状态枚举、错误类型
- `staging/store.py` — 暂存区：去重、容量与淘汰、校验与回滚、持久化恢复
- `staging/scheduler.py` — 调度器：并发控制、target 互斥、进度与取消
- `main.py` — HTTP 接口层
- `tests/` — 基于本地临时目录与可控假数据源的测试，不依赖外部服务
