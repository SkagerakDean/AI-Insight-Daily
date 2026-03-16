#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import requests
import yaml


API_BASE = "https://open.feishu.cn/open-apis"
REPO_ROOT = Path(__file__).resolve().parents[1]


class FeishuError(RuntimeError):
    pass


@dataclass(frozen=True)
class Article:
    kind: str
    path: Path
    title: str
    markdown: str
    notification_summary: str


class FeishuClient:
    def __init__(self, app_id: str, app_secret: str) -> None:
        self.app_id = app_id
        self.app_secret = app_secret
        self.session = requests.Session()
        self._tenant_access_token: str | None = None

    def _auth_headers(self) -> dict[str, str]:
        if not self._tenant_access_token:
            response = self.session.post(
                f"{API_BASE}/auth/v3/tenant_access_token/internal",
                json={"app_id": self.app_id, "app_secret": self.app_secret},
                timeout=30,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("code") != 0:
                raise FeishuError(
                    f"failed to get tenant access token: {payload.get('msg', 'unknown error')}"
                )
            self._tenant_access_token = payload["tenant_access_token"]
        return {"Authorization": f"Bearer {self._tenant_access_token}"}

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, str] | None = None,
        json_data: dict | None = None,
        data: dict[str, str] | None = None,
        files: dict | None = None,
    ) -> dict:
        headers = self._auth_headers()
        if json_data is not None:
            headers["Content-Type"] = "application/json"
        response = self.session.request(
            method,
            f"{API_BASE}{path}",
            params=params,
            json=json_data,
            data=data,
            files=files,
            headers=headers,
            timeout=60,
        )
        try:
            payload = response.json()
        except ValueError as exc:
            response.raise_for_status()
            raise FeishuError(f"non-json response from Feishu: {response.text[:200]}") from exc
        if payload.get("code") != 0:
            raise FeishuError(f"{payload.get('msg', 'unknown error')} ({payload.get('code')})")
        return payload.get("data", {})

    def get_folder_meta(self, folder_token: str) -> dict:
        return self._request("GET", f"/drive/explorer/v2/folder/{folder_token}/meta")

    def list_folder_files(self, folder_token: str) -> list[dict]:
        files: list[dict] = []
        page_token = ""
        while True:
            params = {"folder_token": folder_token, "page_size": "200"}
            if page_token:
                params["page_token"] = page_token
            data = self._request("GET", "/drive/v1/files", params=params)
            files.extend(data.get("files", []))
            if not data.get("has_more"):
                return files
            page_token = data.get("next_page_token", "")

    def delete_file(self, file_token: str, file_type: str) -> None:
        self._request("DELETE", f"/drive/v1/files/{file_token}", params={"type": file_type})

    def create_document(self, title: str) -> str:
        data = self._request("POST", "/docx/v1/documents", json_data={"title": title})
        return data["document"]["document_id"]

    def append_lines(self, document_id: str, lines: Iterable[str]) -> None:
        batch: list[dict] = []
        for line in lines:
            batch.append(
                {
                    "block_type": 2,
                    "text": {
                        "elements": [
                            {
                                "text_run": {
                                    "content": line,
                                }
                            }
                        ]
                    },
                }
            )
            if len(batch) == 50:
                self._request(
                    "POST",
                    f"/docx/v1/documents/{document_id}/blocks/{document_id}/children",
                    json_data={"children": batch},
                )
                batch = []
        if batch:
            self._request(
                "POST",
                f"/docx/v1/documents/{document_id}/blocks/{document_id}/children",
                json_data={"children": batch},
            )

    def move_file(self, file_token: str, folder_token: str, file_type: str = "docx") -> None:
        self._request(
            "POST",
            f"/drive/v1/files/{file_token}/move",
            json_data={"type": file_type, "folder_token": folder_token},
        )

    def upload_markdown(self, file_name: str, content: str) -> str:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".md", delete=False) as tmp:
            tmp.write(content)
            tmp_path = Path(tmp.name)
        try:
            data = self._request(
                "POST",
                "/drive/v1/medias/upload_all",
                data={
                    "file_name": file_name,
                    "parent_type": "ccm_import_open",
                    "size": str(tmp_path.stat().st_size),
                    "extra": json.dumps({"obj_type": "docx", "file_extension": "md"}),
                },
                files={"file": (file_name, tmp_path.read_bytes(), "text/markdown")},
            )
            return data["file_token"]
        finally:
            tmp_path.unlink(missing_ok=True)

    def create_import_task(self, file_token: str, file_name: str, folder_token: str) -> str:
        data = self._request(
            "POST",
            "/drive/v1/import_tasks",
            json_data={
                "file_extension": "md",
                "file_token": file_token,
                "type": "docx",
                "file_name": file_name,
                "point": {"mount_type": 1, "mount_key": folder_token},
            },
        )
        return data["ticket"]

    def poll_import_task(self, ticket: str) -> None:
        for _ in range(30):
            data = self._request("GET", f"/drive/v1/import_tasks/{ticket}")
            result = data.get("result", {})
            status = result.get("job_status")
            if status == 0:
                return
            if status in {1, 2, 3}:
                time.sleep(2)
                continue
            raise FeishuError(result.get("job_error_msg", f"import failed with status {status}"))
        raise FeishuError("import task timed out")

    def send_message_card(
        self,
        *,
        receive_id: str,
        receive_id_type: str,
        title: str,
        summary: str,
        doc_url: str,
        kind: str,
    ) -> None:
        header_title = f"{'日报' if kind == 'daily' else '周报'}更新通知"
        color = "blue" if kind == "daily" else "green"
        card = {
            "config": {"wide_screen_mode": True},
            "header": {
                "template": color,
                "title": {"tag": "plain_text", "content": header_title},
            },
            "elements": [
                {
                    "tag": "markdown",
                    "content": f"**{title}**\n\n{summary}",
                },
                {
                    "tag": "action",
                    "actions": [
                        {
                            "tag": "button",
                            "text": {"tag": "plain_text", "content": "打开飞书文档"},
                            "type": "primary",
                            "url": doc_url,
                        }
                    ],
                },
            ],
        }
        self._request(
            "POST",
            "/im/v1/messages",
            params={"receive_id_type": receive_id_type},
            json_data={
                "receive_id": receive_id,
                "msg_type": "interactive",
                "content": json.dumps(card, ensure_ascii=False),
            },
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync latest Hugo markdown content to Feishu.")
    parser.add_argument(
        "--mode",
        choices=["latest", "all"],
        default="latest",
        help="Sync only the latest daily/weekly file or all files.",
    )
    parser.add_argument(
        "--kind",
        choices=["all", "daily", "weekly"],
        default="all",
        help="Select which content stream to sync.",
    )
    parser.add_argument("--dry-run", action="store_true", help="Show what would be synced.")
    return parser.parse_args()


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"missing required environment variable: {name}")
    return value


def split_front_matter(text: str) -> tuple[dict, str]:
    if not text.startswith("---\n"):
        return {}, text
    parts = text.split("---\n", 2)
    if len(parts) < 3:
        return {}, text
    front_matter = yaml.safe_load(parts[1]) or {}
    return front_matter, parts[2]


def clean_markdown(body: str) -> str:
    body = body.replace("\r\n", "\n")
    body = re.sub(
        r"^\s*>\s*`AI资讯`.*?(?:\n\s*\n|\n(?=## ))",
        "",
        body,
        count=1,
        flags=re.DOTALL,
    )
    body = re.split(r"\n##\s*\*?\*?AI资讯日报多渠道\*?\*?.*$", body, maxsplit=1, flags=re.MULTILINE)[0]
    body = re.sub(r"<br\s*/?>", "\n", body)
    body = re.sub(
        r"!\[([^\]]*)\]\(([^)]+)\)",
        lambda m: f"[图片链接：{m.group(1) or '查看原图'}]({m.group(2)})",
        body,
    )
    body = re.sub(r"\n{3,}", "\n\n", body).strip()
    return body


def build_markdown(title: str, body: str) -> str:
    cleaned = clean_markdown(body)
    if cleaned.startswith("# "):
        return cleaned + "\n"
    return f"# {title}\n\n{cleaned}\n"


def extract_daily_summary(body: str) -> str:
    match = re.search(r"##\s*\*?\*?今日摘要\*?\*?.*?```(.*?)```", body, flags=re.DOTALL)
    if not match:
        return ""
    lines = [line.strip() for line in match.group(1).splitlines() if line.strip()]
    if not lines:
        return ""
    formatted = "\n".join(f"- {line}" for line in lines)
    return formatted[:900]


def extract_weekly_summary(body: str) -> str:
    lines: list[str] = []
    for raw_line in body.splitlines():
        stripped = raw_line.strip()
        if stripped.startswith("> **期刊."):
            lines.append(stripped.lstrip("> ").strip())
        elif stripped.startswith("> **本周关键词**"):
            lines.append(stripped.lstrip("> ").strip())
        elif stripped.startswith("> **主编寄语**"):
            lines.append(stripped.lstrip("> ").strip())
            break
    return "\n".join(lines)


def load_article(path: Path, kind: str) -> Article:
    front_matter, body = split_front_matter(path.read_text(encoding="utf-8"))
    title = (
        str(front_matter.get("title") or front_matter.get("linkTitle") or path.stem)
        .replace("何夕2077", "博观AI资讯")
        .strip()
    )
    notification_summary = (
        extract_daily_summary(body) if kind == "daily" else extract_weekly_summary(body)
    )
    return Article(
        kind=kind,
        path=path,
        title=title,
        markdown=build_markdown(title, body),
        notification_summary=notification_summary,
    )


def daily_files() -> list[Path]:
    base = REPO_ROOT / "content" / "cn"
    matches = [
        path
        for path in base.glob("[0-9][0-9][0-9][0-9]-[0-9][0-9]/*.md")
        if path.name != "_index.md"
    ]
    return sorted(matches)


def weekly_files() -> list[Path]:
    base = REPO_ROOT / "content" / "cn" / "blog" / "weekly"
    matches = [path for path in base.glob("*.md") if path.name != "_index.md"]
    return sorted(matches)


def pick_articles(mode: str, kind: str) -> list[Article]:
    selected: list[Article] = []
    if kind in {"all", "daily"}:
        paths = daily_files()
        if mode == "latest" and paths:
            paths = [paths[-1]]
        selected.extend(load_article(path, "daily") for path in paths)
    if kind in {"all", "weekly"}:
        paths = weekly_files()
        if mode == "latest" and paths:
            paths = [paths[-1]]
        selected.extend(load_article(path, "weekly") for path in paths)
    return selected


def delete_same_title_docs(client: FeishuClient, folder_token: str, title: str) -> None:
    for item in client.list_folder_files(folder_token):
        if item.get("name") == title and item.get("type") == "docx":
            client.delete_file(item["token"], "docx")


def find_doc_url(client: FeishuClient, folder_token: str, title: str) -> str:
    for item in client.list_folder_files(folder_token):
        if item.get("name") == title and item.get("type") == "docx":
            return item.get("url", "")
    raise FeishuError(f"unable to locate synced doc url for {title}")


def sync_article(client: FeishuClient, article: Article, folder_token: str) -> str:
    delete_same_title_docs(client, folder_token, article.title)
    uploaded_file = client.upload_markdown(f"{article.title}.md", article.markdown)
    try:
        for attempt in range(3):
            try:
                ticket = client.create_import_task(uploaded_file, article.title, folder_token)
                client.poll_import_task(ticket)
                break
            except FeishuError as exc:
                if "resource contention occurred" not in str(exc) or attempt == 2:
                    raise
                time.sleep(2)
    except Exception:
        raise
    return find_doc_url(client, folder_token, article.title)


def main() -> int:
    args = parse_args()
    app_id = require_env("FEISHU_APP_ID")
    app_secret = require_env("FEISHU_APP_SECRET")
    daily_folder = require_env("FEISHU_DAILY_FOLDER_TOKEN")
    weekly_folder = require_env("FEISHU_WEEKLY_FOLDER_TOKEN")
    notify_receive_ids = [x.strip() for x in os.environ.get("FEISHU_NOTIFY_RECEIVE_IDS", "").split(",") if x.strip()]
    notify_id_type = os.environ.get("FEISHU_NOTIFY_ID_TYPE", "user_id").strip() or "user_id"

    articles = pick_articles(args.mode, args.kind)
    if not articles:
        print("No matching articles found.")
        return 0

    print(f"Found {len(articles)} article(s) to sync.")
    for article in articles:
        print(f"- {article.kind}: {article.title} ({article.path.relative_to(REPO_ROOT)})")
    if args.dry_run:
        return 0

    client = FeishuClient(app_id, app_secret)
    client.get_folder_meta(daily_folder)
    client.get_folder_meta(weekly_folder)

    for article in articles:
        folder_token = daily_folder if article.kind == "daily" else weekly_folder
        try:
            doc_url = sync_article(client, article, folder_token)
            if notify_receive_ids:
                for receive_id in notify_receive_ids:
                    try:
                        client.send_message_card(
                            receive_id=receive_id,
                            receive_id_type=notify_id_type,
                            title=article.title,
                            summary=article.notification_summary,
                            doc_url=doc_url,
                            kind=article.kind,
                        )
                    except FeishuError as exc:
                        print(
                            f"Warning: failed to send notification to {receive_id} "
                            f"({notify_id_type}): {exc}"
                        )
            print(f"Synced {article.kind}: {article.title}")
        except FeishuError as exc:
            if "destination parent no permission" in str(exc) or "mount_no_permission" in str(exc):
                raise SystemExit(
                    "Feishu app cannot write into the target folder. "
                    "Grant the app edit permission to that folder, then rerun.\n"
                    f"folder_token={folder_token}\nerror={exc}"
                )
            raise SystemExit(f"Failed to sync {article.title}: {exc}") from exc

    return 0


if __name__ == "__main__":
    sys.exit(main())
