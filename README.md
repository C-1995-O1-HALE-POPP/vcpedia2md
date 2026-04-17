# VCPedia 增量抓取说明

本项目用于增量抓取 VCPedia 页面，直接通过 MarkItDown 将页面 URL 转成 Markdown，并按需通过 OpenAI 兼容接口生成页面摘要。

核心实现见 [main.py](main.py)。

## URL 转 Markdown Demo

如果你想快速验证“给一个链接输出 Markdown”，可以用 [markitdown_url_demo.py](markitdown_url_demo.py)：

先安装依赖：

```bash
pip install -r requirements.txt
```

直接输出到终端：

```bash
python markitdown_url_demo.py "https://vcpedia.cn/zh-hans/%E6%B4%9B%E5%A4%A9%E4%BE%9D"
```

输出到文件：

```bash
python markitdown_url_demo.py "https://vcpedia.cn/zh-hans/%E6%B4%9B%E5%A4%A9%E4%BE%9D" -o research/demo-luotianyi.md
```

## 1. 抓取与摘要概览

页面级 summary：
- 抓取到页面后，写入 Markdown Front Matter 的 `summary` 字段
- 同步写入索引文件 [research/vcpedia-pages-index.jsonl](research/vcpedia-pages-index.jsonl) 的 `summary` 字段
- 页面较短时直接使用本地 `short_summary`
- 未配置 API key 时自动全量回退到本地 `short_summary`
- 相同标题与内容命中缓存时，复用 [research/vcpedia-summary-cache.json](research/vcpedia-summary-cache.json) 中的摘要
- LLM 调用失败时自动回退到本地摘要，保证流程不中断

运行级 summary：
- 每次运行结束输出统计日志
- 可选通过 ServerChan3 发送通知
- 通知内容包含抓取结果、模型信息、token 用量、过滤计数、缓存命中数

## 2. 运行统计字段

每次 run 会统计以下内容：
- `scanned_this_run`
- `fetched_this_run`
- `failed_this_run`
- `deferred_retry_this_run`
- `visited_total`
- `failed_total`
- `queue_size`
- `llm_success_count`
- `llm_fallback_count`
- `llm_cache_hit_count`
- `enqueue_skipped_by_filter`
- `prompt_tokens`
- `completion_tokens`
- `total_tokens`

## 3. LLM 总结与效率优化

LLM 接口：
- 使用 openai Python SDK
- 调用 `chat.completions.create`

当前已做的效率优化：
- 内部固定使用较短上下文和较短摘要输出
- 短页面直接走本地摘要
- 相同内容复用本地缓存
- 没有配置 `--llm-api-key` 时不再报错，直接走本地摘要

限流机制：
- 本地请求节流器 `RequestRateLimiter`
- 通过 `--llm-qps` 控制每秒请求数
- 当 `--llm-qps <= 0` 时关闭节流

常用参数：
- `--llm-base-url`
- `--llm-api-key`
- `--llm-model`
- `--llm-timeout`
- `--llm-qps`
- `--fetch-timeout`
- `--fetch-batch-size`
- `--ignore-env-proxy`

## 4. Explore Filter

你可以通过 JSON 配置正则表达式，统一控制哪些 URL 不允许访问，也不允许从已抓取页面继续扩列入队。

默认配置文件是 [research/vcpedia-explore-filter.json](research/vcpedia-explore-filter.json)。

支持两种写法：
- 直接写成正则表达式数组
- 或写成对象：`{"deny_patterns": [...]}`

示例：

```json
{
  "deny_patterns": [
    "Bilibili",
    "Special:RecentChanges"
  ]
}
```

行为说明：
- 当前待抓取 URL 只要匹配任一正则，就直接拒绝访问
- 页面里提取出的新 URL 只要匹配任一正则，就直接拒绝入队
- 匹配使用 Python `re.search`，所以 URL 中包含该表达式即可命中

## 5. 结束通知

支持在任务结束时发送 ServerChan3 通知。

参数：
- `--serverchan3-sendkey`
- `--serverchan3-endpoint`
- `--notify-on-failure`

环境变量：
- `SERVERCHAN3_SENDKEY`
- `SERVERCHAN3_ENDPOINT`

通知内容包含：
- 运行状态
- 模型名
- 抓取统计
- 过滤计数
- 缓存命中数
- token 用量
- 失败时的错误信息

## 6. 最小运行示例

先设置环境变量：

```bash
export OPENAI_BASE_URL="https://api.openai.com/v1"
export OPENAI_API_KEY="你的key"
export OPENAI_MODEL="gpt-4o-mini"
export SERVERCHAN3_SENDKEY="你的sendkey"
```

执行一次增量抓取：

```bash
python - <<'PY'
import json
from pathlib import Path

path = Path("research/vcpedia-crawl-state.json")
state = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}
state["queue"] = ["https://vcpedia.cn/zh-hans/%E6%B4%9B%E5%A4%A9%E4%BE%9D"]
state.setdefault("visited", [])
state.setdefault("failed", [])
state.setdefault("failure_meta", {})
path.write_text(json.dumps(state, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY

python main.py \
  --per-run 20 \
  --llm-qps 1.0 \
  --fetch-timeout 30 \
  --explore-filter-json "research/vcpedia-explore-filter.json" \
  --notify-on-failure
```

如果要在爬虫结束后自动把生成的页面上传到知识库，再补上知识库参数：

```bash
export ASTRBOT_BASE_URL="http://127.0.0.1:6185"
export ASTRBOT_KB_ID="你的知识库ID"
export ASTRBOT_TOKEN="你的JWT token"

python main.py \
  --per-run 20 \
  --llm-qps 1.0 \
  --fetch-timeout 30 \
  --explore-filter-json "research/vcpedia-explore-filter.json" \
  --kb-id "$ASTRBOT_KB_ID" \
  --kb-base-url "$ASTRBOT_BASE_URL" \
  --kb-token "$ASTRBOT_TOKEN" \
  --kb-progress-file "research/vcpedia-kb-upload-state.json" \
  --notify-on-failure
```

默认会上传 [research/vcpedia-pages-md](research/vcpedia-pages-md) 下生成的 Markdown 文件。
上传进度会记录在 [research/vcpedia-kb-upload-state.json](research/vcpedia-kb-upload-state.json)；如果某些文档上次未成功上传，下次运行会自动补传。

## 7. 产物位置

- 页面 Markdown： [research/vcpedia-pages-md](research/vcpedia-pages-md)
- 索引 JSONL： [research/vcpedia-pages-index.jsonl](research/vcpedia-pages-index.jsonl)
- 爬虫状态： [research/vcpedia-crawl-state.json](research/vcpedia-crawl-state.json)
- 摘要缓存： [research/vcpedia-summary-cache.json](research/vcpedia-summary-cache.json)
- 过滤规则： [research/vcpedia-explore-filter.json](research/vcpedia-explore-filter.json)

## 8. 常见问题

1. LLM 报错导致中断吗？

不会。单页摘要失败会回退到本地 `short_summary`，不影响整体 crawl。

2. 为什么没有收到通知？

先检查 `sendkey` 是否正确；未配置 `sendkey` 时会跳过通知发送。

3. 如何屏蔽某类 URL？

把能匹配该 URL 的正则写进 `deny_patterns`，例如 `Bilibili` 或 `Special:RecentChanges`。

4. 现在怎么设置起始页面？

直接手动编辑 [research/vcpedia-crawl-state.json](research/vcpedia-crawl-state.json) 里的 `queue`。程序不再读取 `--start-url`，也不会自动补 seed。

5. 为什么看起来卡在 `0/3000`？

先加 `--verbose` 看详细日志。当前版本会把 `--per-run` 作为“本轮最多尝试多少个 URL”，并默认启用 `--fetch-timeout 30` 与较小批次抓取，避免单个 URL 长时间阻塞整批任务。

6. 代理环境会影响抓取吗？

会。`requests` 默认会读取 `HTTP(S)_PROXY` / `ALL_PROXY`。如果机器上的代理地址失效，可以加 `--ignore-env-proxy` 绕过环境代理后再跑。
