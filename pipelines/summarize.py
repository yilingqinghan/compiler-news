import os, json, requests
from pipelines.util import ensure_tables, pg_conn
from dotenv import load_dotenv
load_dotenv()

LLM_PROVIDER = os.getenv("LLM_PROVIDER", "ollama")
OLLAMA_HOST  = os.getenv("OLLAMA_HOST", "http://localhost:11434")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE    = os.getenv("OPENAI_BASE", "https://api.openai.com/v1")
OPENAI_MODEL   = os.getenv("OPENAI_MODEL", "gpt-4o-mini")

SCHEMA = {
  "type":"object",
  "properties":{
    "title":{"type":"string"},
    "context":{"type":"string"},
    "key_points":{"type":"array","items":{"type":"string"}},
    "impact":{"type":"string"},
    "links":{"type":"array","items":{"type":"string"}},
    "tags":{"type":"array","items":{"type":"string"}}
  },
  "required":["title","key_points","links"]
}

PROMPT_TMPL = """
你是资深编译器领域情报分析师。
请针对同一事件簇的多条材料生成结构化 JSON（符合此 Schema）：{schema}
重点包含：项目/组件、目标架构、优化/IR/Pass 名称、版本/Release、回归/性能影响、相关 PR/Commit/讨论链接。

材料：
{materials}

只输出 JSON。
"""

def llm_summarize(text: str) -> dict:
    if LLM_PROVIDER == "ollama":
        payload = {
            "model": OLLAMA_MODEL,
            "prompt": text,
            "stream": False,
            "options": {
                "num_ctx": 8192,      # 上下文加大，避免截断
                "temperature": 0.2,   # 更稳定的摘要风格
                "top_p": 0.9,
                "num_thread": 10      # M3 Max 多核并行
                # "num_gpu": 1        # 一般不手动设置，留默认
            }
        }
        r = requests.post(f"{OLLAMA_HOST}/api/generate", json=payload, timeout=180)
        r.raise_for_status()
        out = r.json().get("response","").strip()
    else:
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}"}
        data = {
            "model": OPENAI_MODEL,
            "messages":[{"role":"user","content": text}],
            "temperature":0.2
        }
        r = requests.post(f"{OPENAI_BASE}/chat/completions", headers=headers, json=data, timeout=180)
        r.raise_for_status()
        out = r.json()["choices"][0]["message"]["content"]

    try:
        start, end = out.find("{"), out.rfind("}")
        return json.loads(out[start:end+1])
    except Exception:
        return {
            "title":"(parse failed)",
            "key_points":[out[:200]],
            "links":[],
            "tags":[],
            "context":"",
            "impact":""
        }

def main():
    ensure_tables()
    conn = pg_conn(); cur = conn.cursor()
    cur.execute("""
    SELECT cluster_id, array_agg(id) AS ids
    FROM clusters
    GROUP BY cluster_id
    ORDER BY MIN(created_at) DESC
    LIMIT 20;
    """)
    groups = cur.fetchall()

    cur2 = conn.cursor()
    for cid, id_list in groups:
        in_cur = conn.cursor()
        in_cur.execute("SELECT title, url, text FROM articles_clean WHERE id = ANY(%s)", (id_list,))
        rows = in_cur.fetchall(); in_cur.close()
        materials = ""; links = []
        for title, url, text in rows:
            materials += f"\n- {title}\n  {url}\n  {(text or '')[:1000]}...\n"
            links.append(url)

        prompt = PROMPT_TMPL.format(schema=json.dumps(SCHEMA, ensure_ascii=False),
                                    materials=materials[:8000])
        js = llm_summarize(prompt)
        # 合并链接
        js["links"] = list({*(js.get("links") or []), *links})

        for _id in id_list:
            cur2.execute("UPDATE clusters SET summary=%s WHERE id=%s",
                         (json.dumps(js, ensure_ascii=False), _id))
        conn.commit()

    cur2.close(); cur.close(); conn.close()
    print("[summarize] done")

if __name__ == "__main__":
    main()
