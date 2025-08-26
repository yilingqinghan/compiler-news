#!/usr/bin/env python3
import os, sys, socket, subprocess, http.client
from urllib.parse import urlparse

PG_DSN       = os.getenv("PG_DSN", "postgresql://postgres:postgres@localhost:5432/compiler")
MEILI_HOST   = os.getenv("MEILI_HOST", "http://localhost:7700").rstrip("/")
LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama").lower()
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434").rstrip("/")

def tcp_ready(host, port, timeout=1.0):
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except Exception:
        return False

def http_ok(url, timeout=1.5):
    try:
        u = urlparse(url)
        conn = http.client.HTTPConnection(u.hostname, u.port or (80 if u.scheme=="http" else 443), timeout=timeout)
        path = u.path or "/"
        conn.request("GET", path)
        r = conn.getresponse()
        return 200 <= r.status < 500
    except Exception:
        return False

def cmd_ok(cmd, args):
    try:
        out = subprocess.run([cmd]+args, capture_output=True, text=True, timeout=2)
        return out.returncode == 0, (out.stdout + out.stderr)
    except Exception as e:
        return False, str(e)

def docker_running():
    # 优先 Colima
    ok, out = cmd_ok("colima", ["status"])
    if ok and "Running" in out:
        return True, "colima Running"
    # Docker Desktop / Engine
    ok, out = cmd_ok("docker", ["info"])
    if ok:
        return True, "docker info ok"
    # docker.sock
    if os.path.exists("/var/run/docker.sock"):
        return True, "docker.sock exists"
    return False, "not detected"

def main():
    problems = []

    # 0) Docker（仅作友好提示；不一定必须）
    dk_ok, dk_msg = docker_running()
    if not dk_ok:
        problems.append(
            "⚠️ 未检测到 Docker/Colima 就绪。若你使用容器跑数据库/搜索，请先启动：\n"
            "   - macOS（推荐）：colima start\n"
            "   - 或 Docker Desktop：打开 App 并确保 Engine Running\n"
            "   - 若用本机服务（非容器），可忽略此提示"
        )

    # 1) Postgres 必需
    try:
        u = urlparse(PG_DSN)
        host, port = u.hostname or "localhost", int(u.port or 5432)
        if not tcp_ready(host, port, 1.0):
            problems.append(
                f"❌ 无法连接 PostgreSQL {host}:{port}\n"
                f"   - docker compose:  docker compose up -d postgres\n"
                f"   - 本机（macOS）：  brew services start postgresql@16\n"
                f"   - 配置：PG_DSN={PG_DSN}"
            )
    except Exception as e:
        problems.append(f"❌ 解析 PG_DSN 失败：{e}")

    # 2) Meilisearch（可选）
    if MEILI_HOST and not http_ok(MEILI_HOST + "/health", 1.2):
        problems.append(
            f"ℹ️ Meilisearch 未就绪：{MEILI_HOST}/health\n"
            f"   - 如需搜索页：docker compose up -d meilisearch  或  meilisearch --master-key ...\n"
            f"   - 不需要可忽略；index_search 阶段将自动跳过"
        )

    # 3) Ollama（仅当选用）
    if LLM_PROVIDER == "ollama":
        u = urlparse(OLLAMA_HOST)
        if not tcp_ready(u.hostname or "localhost", u.port or 11434, 1.0):
            problems.append(
                f"ℹ️ Ollama 未就绪：{OLLAMA_HOST}\n"
                f"   - 启动：ollama serve ；首次需拉模型：ollama run llama3.1\n"
                f"   - 或改用 OpenAI：设置 LLM_PROVIDER=openai + OPENAI_API_KEY"
            )

    if problems:
        print("💡 前置检查未通过：\n" + "\n\n".join(problems))
        sys.exit(2)

    print("✅ Preflight OK: 基础设施就绪")
    # 标记本轮流水线已通过前置检查（供后续脚本禁用重复检查）
    print("PREFLIGHT_OK=1")

from pipelines.util import run_cli
if __name__ == "__main__":
    run_cli(main)