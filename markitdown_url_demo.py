#!/usr/bin/env python3
"""Convert a given URL to Markdown using MarkItDown."""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from markitdown import MarkItDown
from openai import OpenAI
load_dotenv()

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Convert a URL to Markdown with MarkItDown")
    parser.add_argument("url", help="Target URL to convert")
    parser.add_argument(
        "-o",
        "--output",
        default="",
        help="Optional output markdown file path; print to stdout when omitted",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()

    openai_api_key = os.getenv("OPENAI_API_KEY")
    openai_base_url = os.getenv("OPENAI_BASE_URL")
    client = OpenAI(
        api_key=openai_api_key,
        base_url=openai_base_url,
    )
    # test client
    try:
        response = client.chat.completions.create(
            model="gpt-5.4-mini",
            messages=[{"role": "user", "content": "Hi!"}],
            max_tokens=20,
        )
        print("OpenAI client test successful:", response.choices[0].message.content)
    except Exception as e:
        print("Error testing OpenAI client:", e, file=sys.stderr)
        return 1
    
    converter = MarkItDown(
        enable_plugins=True,
        llm_client=OpenAI(),
        llm_model="gpt-5.4-mini",
    )
    result = converter.convert(args.url)
    markdown_text = getattr(result, "text_content", "")

    if not markdown_text:
        print("MarkItDown conversion returned empty content.", file=sys.stderr)
        return 2

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(markdown_text, encoding="utf-8")
        print(f"Saved markdown to: {output_path}")
    else:
        print(markdown_text)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
