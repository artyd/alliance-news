"""Telegra.ph publishing — build pages and post them via the Telegraph API.

The daily briefing used to send one dense bullet message per department. The
new format publishes a *plain-language* article per department to Telegra.ph
(explained simply, "as for a layperson") and sends ONE summary Telegram
message whose inline buttons open those articles.

Pure helpers (node building) live at the top and are unit-tested without a
network. The two async functions (`create_account`, `create_page`) take an
`httpx.AsyncClient` supplied by the caller, mirroring how `main.py` already
owns the shared clients.

Telegraph content is an array of Node. A Node is either a plain string or a
NodeElement dict: {"tag": str, "attrs": {...}, "children": [Node, ...]}.
Allowed tags: a, aside, b, blockquote, br, code, em, figcaption, figure, h3,
h4, hr, i, iframe, img, li, ol, p, pre, s, strong, u, ul, video.
Allowed attrs: href, src.
"""

from __future__ import annotations

API_BASE = "https://api.telegra.ph"

# Telegraph rejects content larger than 64 KB. Keep a wide margin.
_MAX_CONTENT_BYTES = 60_000


def _p(text: str) -> dict:
    """A paragraph node."""
    return {"tag": "p", "children": [text]}


def plain_body_to_nodes(body: str) -> list:
    """Turn a plain-text article body into Telegraph nodes.

    Rules (deliberately simple and robust to LLM output):
    - Blank line separates paragraphs.
    - A run of consecutive lines that start with a bullet marker
      ("•", "-", "*", "–") becomes a <ul> of <li>.
    - Everything else becomes a <p>.
    Text is kept as-is; Telegraph escapes it. NO markdown is emitted.
    """
    nodes: list = []
    if not body:
        return nodes
    # Normalise newlines and split into logical blocks on blank lines.
    lines = [ln.rstrip() for ln in body.replace("\r\n", "\n").split("\n")]

    bullet_markers = ("• ", "- ", "* ", "– ")

    def _is_bullet(ln: str) -> bool:
        return ln.lstrip().startswith(bullet_markers)

    i = 0
    n = len(lines)
    while i < n:
        line = lines[i]
        if not line.strip():
            i += 1
            continue
        if _is_bullet(line):
            items = []
            while i < n and _is_bullet(lines[i]):
                raw = lines[i].lstrip()
                for m in bullet_markers:  # strip the leading marker + space
                    if raw.startswith(m):
                        raw = raw[len(m):]
                        break
                items.append({"tag": "li", "children": [raw.strip()]})
                i += 1
            if items:
                nodes.append({"tag": "ul", "children": items})
            continue
        # Otherwise a single non-blank line becomes a paragraph.
        nodes.append(_p(line.strip()))
        i += 1
    return nodes


def sources_to_nodes(sources: list, heading: str = "Джерела") -> list:
    """Build the sources section: an <h4> heading + <ul> of linked titles.

    sources: list of (title, url) tuples. De-duplicated by url. Entries with no
    url are skipped.
    """
    seen: set = set()
    items = []
    for title, url in sources or []:
        if not url or url in seen:
            continue
        seen.add(url)
        label = (title or url).strip()[:120] or url
        items.append({
            "tag": "li",
            "children": [{"tag": "a", "attrs": {"href": url}, "children": [label]}],
        })
    if not items:
        return []
    return [{"tag": "h4", "children": [heading]}, {"tag": "ul", "children": items}]


def build_page_content(body: str, sources: list | None = None,
                       footer: str | None = None) -> list:
    """Assemble the full Telegraph page: body paragraphs, sources, optional footer."""
    content = plain_body_to_nodes(body)
    src = sources_to_nodes(sources or [])
    if src:
        content.append({"tag": "hr"})
        content.extend(src)
    if footer:
        content.append({"tag": "p", "children": [{"tag": "i", "children": [footer]}]})
    if not content:
        content = [_p("—")]
    return content


def _content_within_limit(content: list) -> bool:
    """Rough byte-size guard against Telegraph's 64 KB content cap."""
    import json
    return len(json.dumps(content, ensure_ascii=False).encode("utf-8")) <= _MAX_CONTENT_BYTES


async def create_account(client, short_name: str = "AllianceNews",
                         author_name: str = "Alliance News") -> str | None:
    """Create a throwaway Telegraph account and return its access_token.

    Call once; persist the token (env TELEGRAPH_TOKEN) and reuse it so all pages
    live under the same account. Returns None on failure.
    """
    try:
        r = await client.post(f"{API_BASE}/createAccount", data={
            "short_name": short_name[:32],
            "author_name": author_name[:128],
        })
        j = r.json()
        if j.get("ok"):
            return j["result"].get("access_token")
    except Exception:
        return None
    return None


async def create_page(client, access_token: str, title: str, content: list,
                      author_name: str = "Alliance News") -> str | None:
    """Publish a Telegraph page and return its URL, or None on failure."""
    import json
    if not access_token or not content:
        return None
    if not _content_within_limit(content):
        # Trim from the end until it fits (keep the lead, drop tail nodes).
        while content and not _content_within_limit(content):
            content.pop()
        if not content:
            return None
    try:
        r = await client.post(f"{API_BASE}/createPage", data={
            "access_token": access_token,
            "title": title[:256],
            "author_name": author_name[:128],
            "content": json.dumps(content, ensure_ascii=False),
            "return_content": "false",
        })
        j = r.json()
        if j.get("ok"):
            return j["result"].get("url")
    except Exception:
        return None
    return None
