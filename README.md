# 编译器日报 / 周报（LLVM / GCC / …）📰

自动抓取编译器生态信息源（LLVM、GCC、Rust、Swift、Wasmtime…），去重聚类 → 生成**中文/英文双语**周报页面，支持**标签筛选、关键词检索、一键导出 Markdown/PDF**、“本周概览”AI 写作、**每周/每天归档**、**周览小窗**等。

## TL;DR：10 分钟跑起来 🚀

> macOS / Linux 通用。Windows 建议 WSL2。本项目当前在Mac M3 Max实验。

```bash
# 1) 克隆并进入
git clone <your-repo-url> compiler-intel && cd compiler-intel

# 2) Python 环境
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 3) 依赖服务（任选其一的 LLM；Meilisearch 可选）
# PostgreSQL：确保本地有一个空库（默认连接字符串见 .env 示例）
# LLM a) 本地 Ollama：brew install ollama && ollama run llama3.1
# LLM b) OpenAI：准备 OPENAI_API_KEY
# 搜索（可选）：Meilisearch：brew install meilisearch && meilisearch --master-key master_key_change_me &

# 4) 配置
cp .env.example .env     # 按需要修改连接串/模型配置
# 最快试跑可以用 NO_LLM=1（不用模型，仅生成页面）

# 5) 首次跑一遍（最近 7 天）
TIME_WINDOW_DAYS=7 NO_LLM=1 make run

# 6) 打开页面
open web/dist/weekly-$(date +%F).html   # macOS
# 或者浏览器直接打开 web/dist/weekly-YYYY-MM-DD.html
```

> 想要“带 AI 总结/权重/中文解读”：把 `.env` 里 LLM 参数配置好，再执行 `make weekly`（会调用模型）。

------

## 功能亮点 ✨

- 🧠 **AI 总结**：聚类后为每个话题生成 `one_liner`、`digest`、`importance_reason`，并翻译为中文
- 🗂️ **结构化浏览**：按项目（LLVM/GCC/…）分组，**标题+一句话**首屏即览，细节折叠
- 🧭 **周览小窗**：📅 弹窗查看**一周 7 天**的分布和每天的条目
- 🧷 **标签/架构筛选**：点击 badge 即筛选；支持“紧凑/舒展密度”、“深浅主题”
- 🔎 **快速检索**：输入框秒过滤（标题/摘要/标签/项目）；`/` 一键聚焦
- 🌍 **中英文切换**：源是英文自动中文化
- 📤 **导出**：一键导出 Markdown / 浏览器打印 PDF
- 📈 **统计**：来源柱状图、项目饼图 + **来源表格**
- 🗃️ **归档**：自动生成 `index.html` 归档列表（周报 + 最近 N 天日报）

------

## 项目结构 🧱

```
pipelines/         # 抓取/抽取/聚类/总结/发布
  ingest_rss.py
  extract.py
  dedupe_cluster.py
  summarize.py
  publish.py
  publish_weekly.py
  index_search.py
  util.py
web/
  templates/weekly.html.j2   # 前端模板（Tailwind + daisyUI + Chart.js）
  dist/                      # 生成的静态页输出目录
scripts/
  run_daily.sh               # 每日小步抓取
  run_weekly.sh              # 每周汇总
  build_archive.py           # 自动生成归档 index.html
Makefile
.env.example
```

------

## 依赖与环境 🧩

- Python 3.11+（建议 3.12）
- PostgreSQL（默认连接串见 `.env.example`）
- **LLM 二选一**
  - 本地 **Ollama**（默认 `llama3.1`）：低成本、离线可用
  - **OpenAI**（`gpt-4o-mini` 等）：效果更稳定
- （可选）**Meilisearch**：用于独立搜索页和前端检索增强

------

## 配置 `.env` 🔧

```ini
# Database
PG_DSN=postgresql://user:password@127.0.0.1:5432/compiler

# Time window (days) for summarize/publish
TIME_WINDOW_DAYS=7

# LLM provider
LLM_PROVIDER=ollama      # ollama | openai
# Ollama
OLLAMA_HOST=http://localhost:11434
OLLAMA_MODEL=llama3.1
# OpenAI
OPENAI_API_KEY=sk-xxxx
OPENAI_BASE=https://api.openai.com/v1
OPENAI_MODEL=gpt-4o-mini

# Meilisearch (optional)
MEILI_HOST=http://localhost:7700
MEILI_MASTER_KEY=master_key_change_me
```

> **只想快速看页面**：可以设置 `NO_LLM=1` 运行（不会调用模型，也就没有“理由/中文解读”等高级字段）。

------

## 常用命令 🛠️

```bash
# 全流程（7 天窗口）
make run
# 仅重渲染 UI（模板改动后很快）
make ui
# 复用数据库/摘要，重发页面 + 搜索
make republish
# 每日（小步抓取，默认 1 天窗口）
make daily
# 每周（汇总 + 发布 + 索引 + 归档）
make weekly
# 归档 index.html（从 web/dist 扫描）
make archive
```

> 环境开关：
>
> - `TIME_WINDOW_DAYS=7 NO_LLM=1 make run`（不调模型）
> - `TIME_WINDOW_DAYS=1 make daily`（每日）

------

## 自动化部署（可选）⏱️

### macOS（launchd）

```bash
# 加载
launchctl load -w ~/Library/LaunchAgents/com.compilerintel.daily.plist
launchctl load -w ~/Library/LaunchAgents/com.compilerintel.weekly.plist
```

> `scripts/run_daily.sh`：每天多次；`scripts/run_weekly.sh`：周一 00:10。
>  Linux 用 `cron`，或 GitHub Actions 参考 `.github/workflows/weekly.yml`。

------

## 数据源与覆盖率 📡

- 已内置：**llvm/llvm-project**（commits/PRs）、**LLVM Discourse**（多个分类）、官方 Releases、以及若干外部权威聚合（如 Phoronix/LLVM Weekly）
- 想“只看 LLVM / GCC”：在 `pipelines/ingest_rss.py` 的 FEEDS 列表中**注释/启用**对应条目即可（保持纯净）
- 想确认“是否全面”：可运行（可选）**来源健康检查**脚本（统计 7 天每日条数/空天）与**外部基准对齐**（和 LLVM Weekly/Phoronix 的标题集合做相似度对比）
  - 👉 详情可见 `docs/source-health.md`（自检指标与图表示例）

## 使用技巧 💡

- **语言**：右上角「中文 / English」即时切换
- **搜索**：顶部输入框秒过滤（`/` 聚焦）；支持简单 DSL：
   `proj:LLVM tag:RISC-V p:high -tag:NFC`
- **筛选**：点击任意 tag badge 即筛选该标签
- **周览**：右下角「📅」查看一周每天条数和当日列表
- **导出**：右上角「📝 导出MD」「🖨 打印」
- **收藏**：卡片右上角「☆」加入本地 watchlist
- **快捷键**：`/` 聚焦搜索 · `gg` 回到顶部

------

## 复现一份“只含 LLVM/GCC 的周报” 🍳

```bash
# 1) 编辑 pipelines/ingest_rss.py → 保留 LLVM/GCC 源
# 2) 清理 & 跑
. .venv/bin/activate
TIME_WINDOW_DAYS=7 make run
open web/dist/weekly-$(date +%F).html
```

------

## 故障排查 🧯

- **Meilisearch 502 / 未就绪**
   启动：`meilisearch --master-key master_key_change_me &`
   如果不需要搜索页，忽略 `index_search` 阶段也 OK。
- **没有中文解读/理由**
   说明当次用了 `NO_LLM=1` 或模型失败。配置好 `.env` 后执行 `make weekly`。
- **页面空白/内容很少**
   检查 `TIME_WINDOW_DAYS` 与系统时间；或第一次可把窗口拉到 14~21 天看看效果。
- **PostgreSQL 连接失败**
   确认 `.env` 的 `PG_DSN` 正确，数据库存在并可读写。

------

## 贡献 🤝

欢迎 PR：

- 新的数据源（RSS/API）
- 新的 UI/交互（例如 Meili 高级检索页、对比抽屉、趋势 sparkline）
- Source Health 可视化

------

## 进阶文档 📚

- `docs/sources.md` —— 数据源清单与新增指引
- `docs/source-health.md` —— 覆盖率/健康度自检方法
- `docs/search.md` —— Meilisearch 高级检索页（DSL 与高亮）
- `docs/publishing.md` —— 自动化发布（cron/launchd/Actions）

> 还没写？可以先建占位页，后续逐步充实～ 😉

------

## License 📄

Apache2.0
