# VCPedia 爬虫总结功能说明

本项目用于增量抓取 VCPedia 页面，并通过 OpenAI 兼容接口生成页面摘要。
本 README 聚焦“总结功能”，包含两部分：
- 页面级总结：每个页面的 summary 字段
- 运行级总结：每次任务结束后的统计与通知

核心实现见 [main.py](main.py)。

## 1. 总结功能概览

页面级总结：
- 抓取到页面后，调用 LLM 生成中文摘要
- 摘要写入页面 Markdown Front Matter 的 summary 字段
- 摘要也写入索引文件 [research/vcpedia-pages-index.jsonl](research/vcpedia-pages-index.jsonl) 的 summary 字段
- 若 LLM 调用失败，自动回退到本地 short_summary，保证流程不中断

运行级总结：
- 每次运行结束输出统计日志
- 可选通过 ServerChan3 发送通知
- 通知内容包含本次抓取结果、模型信息、token 用量、过滤计数

## 2. 运行统计字段

每次 run 会统计以下内容：
- 扫描数量：scanned_this_run
- 抓取成功：fetched_this_run
- 抓取失败：failed_this_run
- 重试延后：deferred_retry_this_run
- 已访问总量：visited_total
- 失败总量：failed_total
- 队列剩余：queue_size
- LLM 成功次数：llm_success_count
- LLM 回退次数：llm_fallback_count
- 入队过滤数量：enqueue_skipped_by_filter
- token 用量：prompt_tokens、completion_tokens、total_tokens

## 3. LLM 总结与限流

LLM 接口：
- 使用 openai Python SDK
- 调用 chat.completions.create

限流机制：
- 本地请求节流器 RequestRateLimiter
- 通过 --llm-qps 控制每秒请求数
- 当 --llm-qps 小于等于 0 时关闭节流

常用参数：
- --llm-base-url
- --llm-api-key
- --llm-model
- --llm-timeout
- --llm-qps
- --summary-context-chars
- --summary-max-tokens

## 4. 手动探索过滤（控制继续探索）

你可以通过 JSON 配置手动控制哪些链接不入队。
默认配置文件是 [research/vcpedia-explore-filter.json](research/vcpedia-explore-filter.json)。

支持三类规则：
- stop_expand_pages
  说明：来源页面本身会抓取，但从该页面提取到的新链接都不入队
- block_links
  说明：全局屏蔽目标链接，不论来源页面是谁
- block_links_by_source
  说明：按来源页面定向屏蔽目标链接

推荐用法：
- 如果你想“这个页面不再继续探索”，把该页面 URL 放到 stop_expand_pages

示例结构：

  {
    "stop_expand_pages": [
      "https://vcpedia.cn/zh-hans/Bilibili"
    ],
    "block_links": [
      "https://vcpedia.cn/zh-hans/Special:RecentChanges"
    ],
    "block_links_by_source": {
      "https://vcpedia.cn/zh-hans/Bilibili": [
        "https://vcpedia.cn/zh-hans/%E5%88%86%E7%B1%BB:Bilibili"
      ]
    }
  }

## 5. 结束通知（ServerChan3）

支持在任务结束时发送通知。

参数：
- --serverchan3-sendkey
- --serverchan3-endpoint
- --notify-on-failure

环境变量：
- SERVERCHAN3_SENDKEY
- SERVERCHAN3_ENDPOINT

通知内容包含：
- 运行状态（成功/失败）
- 模型名
- 抓取统计
- 过滤计数
- token 用量
- 失败时的错误信息

## 6. 最小运行示例

先设置环境变量：

  export OPENAI_BASE_URL="https://api.openai.com/v1"
  export OPENAI_API_KEY="你的key"
  export OPENAI_MODEL="gpt-4o-mini"
  export SERVERCHAN3_SENDKEY="你的sendkey"

执行一次增量抓取：

  python main.py \
    --start-url "https://vcpedia.cn/zh-hans/Template:%E6%B4%9B%E5%A4%A9%E4%BE%9D" \
    --per-run 20 \
    --llm-qps 1.0 \
    --explore-filter-json "research/vcpedia-explore-filter.json" \
    --notify-on-failure

如果要在爬虫结束后自动把生成的页面上传到知识库，再补上知识库参数：

  export ASTRBOT_BASE_URL="http://127.0.0.1:6185"
  export ASTRBOT_KB_ID="你的知识库ID"
  export ASTRBOT_TOKEN="你的JWT token"

  python main.py \
    --start-url "https://vcpedia.cn/zh-hans/Template:%E6%B4%9B%E5%A4%A9%E4%BE%9D" \
    --per-run 20 \
    --llm-qps 1.0 \
    --explore-filter-json "research/vcpedia-explore-filter.json" \
    --kb-id "$ASTRBOT_KB_ID" \
    --kb-base-url "$ASTRBOT_BASE_URL" \
    --kb-token "$ASTRBOT_TOKEN" \
    --kb-progress-file "research/vcpedia-kb-upload-state.json" \
    --notify-on-failure

默认会上传 [research/vcpedia-pages-md](research/vcpedia-pages-md) 下生成的 Markdown 文件。
上传进度会记录在 [research/vcpedia-kb-upload-state.json](research/vcpedia-kb-upload-state.json)；如果某些文档上次未成功上传，下次运行会自动补传。

## 7. 产物位置

- 页面 Markdown： [research/vcpedia-pages-md](research/vcpedia-pages-md)
- 索引 JSONL： [research/vcpedia-pages-index.jsonl](research/vcpedia-pages-index.jsonl)
- 爬虫状态： [research/vcpedia-crawl-state.json](research/vcpedia-crawl-state.json)
- 过滤规则： [research/vcpedia-explore-filter.json](research/vcpedia-explore-filter.json)

## 8. 常见问题

1) LLM 报错导致中断吗？
不会。单页摘要失败会回退到本地 short_summary，不影响整体 crawl。

2) 为什么没有收到通知？
先检查 sendkey 是否正确；未配置 sendkey 时会跳过通知发送。

3) 如何只抓取页面但不继续扩散？
把该页面 URL 加入 stop_expand_pages。
