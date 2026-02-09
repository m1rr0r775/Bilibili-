# 弹幕爬取与数据分析系统（单人版）

默认使用 SQLite，支持抓取、清洗、分析与可视化的端到端闭环，并预留 MySQL/PostgreSQL 切换能力。

## 项目简介

这是一个面向单人本地运行的“弹幕数据分析”小型系统，目标是把一个视频的弹幕从采集到可视化打通闭环：

- 抓取：按任务调度抓取 B 站历史弹幕（支持断点续爬与去重写入）。
- 清洗：文本规范化、噪声/垃圾规则过滤、分词、情感极性（正向/中性/负向）。
- 分析：弹幕量与情感趋势、高能片段、热词（列表+词云）、用户活跃与用户分层、提及网络（@）、热词突增（梗/名场面候选）。
- 展示：FastAPI + 仪表盘页面（ECharts）+ 可导出的静态 HTML 报告。

系统适合用于：做视频内容复盘、找名场面时间点、总结观众情绪变化、提炼热词与梗、观察是否存在刷屏/头部集中等。

## 快速开始

1. 安装依赖

```bash
python -m venv .venv
.venv\\Scripts\\activate
pip install -r requirements.txt
```

2. 配置环境变量（可选）

复制 `.env.example` 为 `.env` 并按需修改。

3. 启动服务

```bash
python -m visualization.dashboard.server
```

打开浏览器访问 `http://127.0.0.1:8000`。

## 使用说明（推荐流程）

### 1）创建抓取任务

在仪表盘输入视频 ID（支持 `BVxxxx` / `av123` / `cid:123`），点击“创建抓取任务”。后台会轮询执行抓取任务，结果写入本地数据库。

也可以直接用接口创建任务：

```bash
curl -X POST "http://127.0.0.1:8000/tasks/crawl" ^
  -H "Content-Type: application/json" ^
  -d "{\"platform\":\"bilibili\",\"video_input\":\"BVxxxxxxxxxx\",\"task_type\":\"history\"}"
```

### 2）查看任务状态

```bash
curl "http://127.0.0.1:8000/tasks/{task_id}"
```

`status` 可能值：`PENDING` / `RUNNING` / `SUCCEEDED` / `FAILED`。

如失败可重试：

```bash
curl -X POST "http://127.0.0.1:8000/tasks/{task_id}/retry"
```

### 3）运行清洗+分析

仪表盘点击“运行清洗+分析”，或调用接口：

```bash
curl -X POST "http://127.0.0.1:8000/analytics/run" ^
  -H "Content-Type: application/json" ^
  -d "{\"platform\":\"bilibili\",\"video_id\":\"BVxxxxxxxxxx\"}"
```

运行完成后，刷新仪表盘即可看到所有图表与摘要。

### 4）导出 HTML 报告

```bash
curl -X POST "http://127.0.0.1:8000/report/html" ^
  -H "Content-Type: application/json" ^
  -d "{\"platform\":\"bilibili\",\"video_id\":\"BVxxxxxxxxxx\"}"
```

返回字段 `path` 是生成的报告文件路径（默认在 `./data/reports/`）。

## 指标名词说明（简版）

- 情感分析：基于分词与情绪词典规则，输出正向/中性/负向；“占比趋势”表示每个时间桶里该情绪占全部弹幕的比例。
- 用户活跃：独立用户数是去重后的发言用户数；Top10 占比表示最活跃的 10 个用户贡献的弹幕比例，用于衡量是否被少数人刷屏。
- 用户分层（用于人群结构粗分）：
  - low：低频用户（0~1 条）
  - normal：普通用户（2~9 条且重复率不高）
  - active：活跃用户（10~49 条）
  - heavy：重度用户（≥50 条）
  - repeat_suspect：疑似重复刷屏（重复率偏高）
  - spam_suspect：疑似刷屏账号（高频且重复率偏高）
- 认知负荷（词数/秒）：每 10 秒桶内的分词总数除以 10，反映信息密度。
- 信息熵：反映词的多样性与均衡程度（越高越“内容丰富”）。
- 热词突增：某个词在短时间内突然大量出现，用“突增强度”排序；常用于发现梗/名场面。
- 提及网络：识别“@某人”的提及关系，统计提及最多的用户与被提及最多的对象。

## 现有接口一览

- GET `/`：仪表盘页面
- GET `/docs`：OpenAPI 文档（FastAPI 自动生成）
- GET `/health`：健康检查
- POST `/tasks/crawl`：创建抓取任务
- GET `/tasks/{task_id}`：查看任务状态/错误信息
- POST `/tasks/{task_id}/retry`：重试失败任务
- POST `/analytics/run`：对指定视频执行“清洗+分析”
- GET `/analytics/time_series`：拉取时间序列指标
- GET `/analytics/summary`：拉取摘要指标
- GET `/pipeline/latest`：查看最近一次 pipeline_run
- POST `/cache/cleanup`：清空本地缓存目录
- POST `/report/html`：生成静态 HTML 报告

## 可以增加的接口（建议清单）

以下为扩展建议（按实用优先级从高到低），适合后续迭代：

### 数据与任务管理
- GET `/videos`：列出已采集的视频列表（platform/video_id/title/更新时间）。
- GET `/tasks`：按状态筛选任务列表（PENDING/RUNNING/FAILED）。
- DELETE `/videos/{platform}/{video_id}`：删除某个视频的 raw/clean/metrics 数据（用于重跑或清理空间）。
- POST `/analytics/run_async`：异步启动分析并返回 job_id（避免长请求阻塞）。

### 指标与可视化增强
- GET `/analytics/metrics`：列出某个视频可用的 metric_name 列表与说明。
- GET `/analytics/highlights`：直接返回高能片段 + 触发热词（用于剪辑/章节生成）。
- GET `/analytics/wordcloud`：返回词云图片/数据（便于导出到报告或第三方工具）。
- GET `/analytics/network/mentions`：返回提及网络的节点/边结构（用于前端画网络图而不是仅摘要）。

### 检索与审计
- GET `/danmu/search?q=...`：按关键词检索弹幕（支持时间范围、用户分层过滤）。
- GET `/danmu/sample`：返回某段时间窗内的原始弹幕样本（用于对照分析结果）。

### 可运维性
- GET `/stats/storage`：显示数据库大小、缓存大小、raw/clean/metrics 行数。
- POST `/tasks/pause`、POST `/tasks/resume`：暂停/恢复后台抓取轮询。



![127.0.0.1_8000_](E:\personal_project\danmu-analysis\127.0.0.1_8000_.png)
