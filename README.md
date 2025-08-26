# 🛠️ 编译器情报周报（Compiler Intel Weekly）

自动聚合 LLVM / GCC / Rust / Zig / Wasmtime 等编译器领域的动态，按周生成漂亮的 HTML 周报 📑。

------

## 📦 环境依赖

在开始之前，你需要准备好：

- **Python 3.12+**（推荐用 `pyenv` / `asdf` 管理版本）
- **pipenv/venv**（虚拟环境管理）
- **Docker**（推荐 macOS 上用 [Colima](https://github.com/abiosoft/colima)，也可以 Docker Desktop）
- **docker compose**（随 Docker 一起安装）
- （可选）本地 `psql` 客户端，用于调试数据库连接（`brew install libpq`）

------

## 🚀 第一次启动（五步走）

1. **克隆仓库**

   ```
   git clone https://github.com/xxx/compiler-intel.git
   cd compiler-intel
   ```

2. **创建虚拟环境**

   ```
   python3 -m venv .venv
   . .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **启动依赖服务（Postgres + Meilisearch）**

   ```
   docker compose up -d postgres meilisearch
   ```

   > ✅ 默认账号/库：`compiler / compiler / compiler_intel`
   >  📂 数据挂载在 `./pgdata`，如果你之前改过账号，删除这个目录重新初始化即可。

4. **检查服务是否就绪**

   ```
   make preflight
   ```

   - 如果看到 ✅ Preflight OK → 一切正常
   - 如果失败，先看 FAQ 部分

5. **跑一次完整流水线**

   ```
   WINDOW_MODE=week_to_date make run
   ```

   输出在 `web/dist/`，比如：

   - `weekly-2025-08-17.html` → 本周周报
   - `index.html` → 首页（自动指向最新周报）

------

## 🗂️ 常见问题（FAQ）

**Q1: 提示 “数据库未就绪或连接失败”？**
 👉 说明 `.env` 的账号密码和 `pgdata` 里实际初始化的账号不一致。

- 解决方案 A（推荐）：删除 `pgdata/` 再 `docker compose up -d postgres` → 用默认账号跑。
- 解决方案 B：用 `docker exec -it compiler-intel-postgres-1 psql -U postgres` 查实际账号，然后改 `.env` 里的 `PG_DSN`。

------

**Q2: Meilisearch 健康检查失败？**
 👉 容器启动需要几秒，稍等或重启：

```
docker compose restart meilisearch
curl http://localhost:7700/health
# 预期 {"status":"available"}
```

------

**Q3: 我只想调试前端，不想每次都跑 LLM 总结？**
 👉 可以用：

```
make fast
```

它会跳过 `summarize` 阶段，只复用老数据库数据。

------

## 🧭 高级用法

- `WINDOW_MODE=week_to_date` → 统计本周（周一到今天）
- `WINDOW_MODE=last_week` → 统计完整的上周
- 可以加新的 RSS 源（`pipelines/rss_sources.yml`）
- 支持搜索页（需要 Meilisearch）

------

## 🎉 完成后你可以

- 打开 `web/dist/index.html` 查看最新周报
- 部署到 GitHub Pages / Vercel / Netlify
- 配合 crontab / Airflow 实现自动化每周运行
