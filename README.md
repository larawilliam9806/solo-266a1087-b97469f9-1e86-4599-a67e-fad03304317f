# Click Lake Erasure API

面向广告数据分析的点击数据湖服务：按日追加上传点击 CSV、本地 Parquet 存储、按日下载，
并支持 GDPR 风格的用户数据清除（erasure）——受理后立即拦截该用户的新数据，后台重写
受影响文件，完成后永久拦截。另提供按 `file_id` 的日期分区纠正接口，用于修复上游把
整批点击误登记到前一天的分区错误。

技术栈：**FastAPI + PyArrow + SQLite**（SQLite 作为状态库，Parquet 作为数据湖格式）。

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

服务监听 `http://localhost:8000`，交互式文档见 `http://localhost:8000/docs`。
数据与状态持久化在命名卷 `clicklake-data` 中。

### 本地开发

```bash
pip install -r requirements-dev.txt
export DATA_DIR=./data STATE_DIR=./state
uvicorn --factory app.main:create_app --reload
```

### 运行测试

```bash
python -m pytest tests/ -q
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/upload/{date}` | 上传当日点击 CSV（multipart 文件字段 `file`），返回接收/拦截/入库行数 |
| `GET` | `/dates` | 列出所有日期（文件数、行数、是否受限） |
| `GET` | `/dates/{date}/download?format=csv\|parquet` | 按日下载；日期受限时返回 `409` |
| `POST` | `/files/{file_id}/correct-date` | 纠正错分区文件：文件内点击的 UTC 日期相同且与登记日期不同时，迁移到正确日期 |
| `POST` | `/erasure` | 提交清除请求 `{"user_id": "..."}`（幂等，重复提交返回同一任务） |
| `GET` | `/erasure/{task_id}` | 查询清除进度 |
| `GET` | `/erasure?user_id=...` | 列出清除任务 |
| `GET` | `/health` | 健康检查 |

### 上传 CSV 格式

表头必须包含 `user_id,page,timestamp`（额外列被忽略）。时间戳支持 ISO-8601
（建议带 `Z` 或时区偏移）与 Unix 秒，统一归一化为 UTC 存储。

```bash
curl -X POST -F "file=@clicks.csv" http://localhost:8000/upload/2026-11-01
# {"date":"2026-11-01","received":4,"invalid":0,"blocked":1,"stored":3,"file_id":"..."}
```

- `received`：CSV 数据行总数；`invalid`：格式非法被跳过的行数
- `blocked`：因清除请求被拦截（未写入）的行数；`stored`：实际入库行数

### 清除流程

```bash
curl -X POST -H "Content-Type: application/json" \
     -d '{"user_id":"alice"}' http://localhost:8000/erasure
# 201 Created（重复提交返回 200 与同一 task_id）

curl http://localhost:8000/erasure/<task_id>
# {"status":"processing","files_total":2,"files_done":1,"restricted_dates":["2026-11-01"],...}
```

任务状态机：`pending → processing → completed`，失败时为 `failed`（自动重试，重启后继续）。

## 日期分区纠正

上游偶发地把整批点击登记到前一天，导致按日下载漏掉当天记录。
`POST /files/{file_id}/correct-date` 以文件内容为准纠正分区（无需请求体，目标日期从
文件内点击时间戳推导）：

```bash
curl -X POST http://localhost:8000/files/<file_id>/correct-date
# {"file_id":"...","old_date":"2026-11-01","new_date":"2026-11-02","rows":2,"users":2}
```

- **受理条件**：文件为 active，且文件内所有点击的 UTC 日期相同、与登记日期不同。
  含多个 UTC 日期的文件返回 `422` 并继续从原（登记）日期供数；登记日期本就正确的
  文件同样 `422`；未知 `file_id` 返回 `404`。
- **清除限制**：源日期或目标日期正被清除任务限制时返回 `409`，清除完成后可重试。
- **生效**：迁移后原日期不再返回这些点击，正确日期立即可下载；文件内所有用户的
  `(用户, 新日期)` 会写入清除索引，之后针对这些用户的清除请求会覆盖新分区。
- **与清除交错**：纠正的"校验 + 写文件 + 登记"与受理清除在同一把提交锁下完成。
  清除先受理则源日期受限、纠正被拒绝；纠正先提交则清除快照纳入新日期并继续跟踪
  该文件（`file_id` 不变）。含被拦截用户的文件其源日期必然受限，因此已拦截用户
  不可能借纠正从新分区出现。
- **失败与重启**：先持久化新文件（tmp + fsync + 原子 rename），再在单个事务里改
  登记，最后才删除旧文件。写盘或登记失败时仍从旧分区完整供数；提交前崩溃留下未
  登记的孤儿文件、提交后崩溃留下旧副本，启动恢复流程都会清理——任何时刻不会
  两天同时供数，也不会丢数据。已开始的下载在日期锁保护下读完数据，可正常结束。

## 清除语义（保证）

- **立即拦截**：请求受理的同一事务内，该用户即进入拦截集合；之后提交的上传直接丢弃其记录。
- **在途上传不写回**：上传的"过滤 → 写 Parquet → 提交状态"与"受理清除"在同一把提交锁下
  完成，两种操作的临界区不会交错，因此在途上传不可能把被清除用户的数据写回。
- **下载限制**：含该用户的日期在任务完成前暂停新下载（旧链接同样 `409`）；已开始的下载
  在日期锁保护下读取完数据后再响应，可正常结束；其他日期的导入与下载不受影响。
- **完成标准**：后台逐个重写受影响文件（tmp + fsync + 原子 rename），删除含目标数据的旧
  文件后才标记完成；其他用户记录完整保留。
- **交错安全**：单一后台 worker 顺序处理任务；文件被重写时，其他任务的待处理记录在
  同一事务内被重定向到新文件，多个清除与导入交错不会漏删、复活或丢失他人记录。
  完成前还有一次全量校验扫描作为兜底。
- **失败与重启**：写盘失败时任务进入 `failed` 并自动重试，期间拦截与日期限制保持有效；
  进程重启后恢复所有未完成任务，并清理临时文件、孤儿文件与未完成的删除。
- **幂等与永久拦截**：每个用户终生对应同一任务（`user_id` 唯一约束），重复提交返回同一
  任务；任务行永不删除，因此完成后该用户被永久拦截。

## 数据布局

```
data/
  date=2026-11-01/
    part-<file_id>.parquet      # 每次上传追加一个 part 文件
state/
  erasure.db                    # SQLite：文件清单、任务、限制日期、用户-日期索引
```

Parquet schema：`user_id: string, page: string, ts: timestamp[us, tz=UTC]`。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATA_DIR` | `./data` | Parquet 数据目录 |
| `STATE_DIR` | `./state` | SQLite 状态目录 |
| `ERASURE_RETRY_DELAY` | `5` | 失败任务重试间隔（秒） |
| `ERASURE_WORKER_POLL` | `0.2` | 后台 worker 轮询间隔（秒） |
