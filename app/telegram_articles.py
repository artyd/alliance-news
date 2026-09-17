"""Telegram-article generation — pure, testable helpers.

The project historically produced a single PDF report. This module supports the
move to *Telegram articles*: one concise message per department (report
category), built from the same structured facts that feed the PDF.

Only pure logic lives here (prompt building, fact serialization, HTML escaping,
message chunking) so it can be unit-tested without a network or DB. The async
orchestration (LLM call, Telegram send, DB) lives in main.py where the shared
clients already exist.
"""

from __future__ import annotations

# Telegram hard limit for a text message. We keep a margin for safety.
TELEGRAM_MSG_LIMIT = 4096
_CHUNK_TARGET = 3900


def escape_html(text: str) -> str:
    """Escape the characters Telegram's HTML parse_mode is sensitive to."""
    if not text:
        return ""
    return (
        text.replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def build_facts_payload(facts: list[dict], max_facts: int = 40) -> str:
    """Serialize department facts into a compact, deterministic block for the LLM.

    Facts are already ordered by relevance/confidence by the caller. We cap the
    count to keep the prompt bounded and the signal high.
    """
    lines: list[str] = []
    n = 0
    for f in facts[:max_facts]:
        what = (f.get("what_happened") or "").strip()
        if not what:
            continue
        n += 1
        parts = [f"{n}. {what}"]
        who = (f.get("who") or "").strip()
        where = (f.get("where_loc") or "").strip()
        mag = (f.get("magnitude") or "").strip()
        impact = (f.get("supply_chain_impact") or "").strip()
        rel = (f.get("ukraine_relevance") or "").strip()
        if who:
            parts.append(f"   who: {who}")
        if where:
            parts.append(f"   where: {where}")
        if mag:
            parts.append(f"   magnitude: {mag}")
        if impact:
            parts.append(f"   supply-chain impact: {impact}")
        if rel:
            parts.append(f"   ukraine relevance: {rel}")
        lines.append("\n".join(parts))
    return "\n".join(lines)


def bucket_facts_by_department(facts: list[dict], departments: list[dict]) -> dict:
    """Group a flat list of facts into business departments.

    A fact joins a department if ANY of its `affected_sectors` is in the
    department's `sectors`, OR its `event_type` is in the department's
    `event_types`. One fact may land in several departments (that's fine — a
    new tariff on API is both 'laws' and 'procurement'). Order is preserved
    (facts are already relevance-sorted) and de-duplicated by fact id.

    departments: [{"code","name","sectors":[...],"event_types":[...]}, ...]
    Returns {code: [fact, ...]}.
    """
    out: dict[str, list[dict]] = {d["code"]: [] for d in departments}
    seen: dict[str, set] = {d["code"]: set() for d in departments}
    for f in facts:
        sectors = {s.strip() for s in (f.get("affected_sectors") or "").split(",") if s.strip()}
        etype = (f.get("event_type") or "").strip()
        fid = f.get("id")
        for d in departments:
            code = d["code"]
            match = bool(sectors & set(d.get("sectors", []))) or etype in set(d.get("event_types", []))
            if match and fid not in seen[code]:
                out[code].append(f)
                seen[code].add(fid)
    return out


def build_synthesis_prompt(dept_name: str, lang: str = "ua") -> str:
    """System prompt: turn a department's facts into a short Telegram briefing.

    Output is plain text with short paragraphs / bullet lines — NO markdown
    headings, NO tables (Telegram HTML supports neither). The caller wraps it in
    the final HTML envelope.
    """
    lang_name = {"ua": "Ukrainian", "en": "English"}.get(lang, "Ukrainian")
    action_word = "Дія" if lang == "ua" else "Action"
    return (
        "You are a senior B2B procurement & market-intelligence analyst for a "
        "Ukrainian importer of pharmaceutical and chemical raw materials. Write a "
        f"concrete, actionable briefing for the '{dept_name}' department, in {lang_name}.\n\n"
        "HARD RULES:\n"
        "- Use ONLY the facts provided. If nothing is material, reply with exactly "
        "the single word: SKIP.\n"
        "- 4-7 bullets, most important first. Each bullet MUST have three parts:\n"
        "  1) LEAD with the concrete datum from the facts — a number, %, price, "
        "date, company, country or volume. Never open with a vague phrase.\n"
        "  2) Then the concrete consequence for OUR sourcing / logistics / costs / "
        "lead times.\n"
        f"  3) End with '→ {action_word}:' and ONE specific step the team should take "
        "(e.g. lock in a price now, qualify an alternative supplier, pre-order "
        "buffer stock, expedite a shipment, re-check a contract clause, switch "
        "route). Make it realistic and specific to the datum.\n"
        "- If several facts are the same story, MERGE them into one richer bullet "
        "keeping all numbers; do not repeat.\n"
        "- NEVER invent a number that is not in the facts. If a fact has no number, "
        "still name the concrete actor/event and give the action.\n"
        "- Start each bullet with '• '. No headings, no preamble, no closing "
        "summary. Plain text only — no markdown, asterisks or '#'.\n"
        "- Be dense and skimmable; a busy buyer reads this on a phone."
    )


def build_plain_article_prompt(dept_name: str, lang: str = "ua") -> str:
    """System prompt: turn a department's facts into a PLAIN-LANGUAGE article.

    Unlike `build_synthesis_prompt` (terse '→ Дія' bullets for a Telegram
    message), this produces a friendly, easy-to-read article for a Telegra.ph
    page — explained as if for someone with no background, but still concrete.

    The model must return a JSON object:
      {"skip": false,
       "title":   "<short headline, <=80 chars>",
       "teaser":  "<one plain sentence: what happened + why it matters to us>",
       "article": "<plain text: 2-5 short paragraphs, simple words. May use\n
                    '• ' bullet lines. Keep every concrete number/price/date\n
                    from the facts. End with a short 'Що робимо:' takeaway.>"}
    If nothing is material, return {"skip": true}.
    """
    lang_name = {"ua": "Ukrainian", "en": "English"}.get(lang, "Ukrainian")
    takeaway = "Що робимо" if lang == "ua" else "What we do"
    return (
        "You are a market-intelligence analyst for a Ukrainian importer of "
        "pharmaceutical and chemical raw materials. Turn the facts for the "
        f"'{dept_name}' department into a SHORT, PLAIN-LANGUAGE article a busy "
        f"non-expert can understand in one read. Write in {lang_name}.\n\n"
        "Return ONLY a JSON object with keys: skip (bool), title, teaser, article.\n"
        "HARD RULES:\n"
        "- Use ONLY the facts provided. If nothing is material, return "
        '{"skip": true}.\n'
        "- title: a short, human headline (no clickbait), max 80 chars.\n"
        "- teaser: ONE simple sentence — what happened and why it matters to our "
        "sourcing/logistics/costs. This is shown next to a button.\n"
        "- article: 2-5 SHORT paragraphs in simple words, like explaining to a "
        "friend. Plain text only (no markdown, no '#', no HTML). You MAY use lines "
        "starting with '• ' for a short list. Keep EVERY concrete number, %, "
        "price, date, company and country from the facts, but explain what each "
        "means in practice. Never invent numbers.\n"
        f"- End the article with one line starting '{takeaway}:' giving ONE "
        "specific, realistic step for the team.\n"
        "- Be warm and clear, not bureaucratic. A reader with no finance "
        "background should fully get it."
    )


def build_digest_message(items: list[dict], date_str: str,
                         lang: str = "ua") -> tuple[str, list]:
    """Assemble the ONE summary Telegram message + its inline keyboard.

    items: [{"name": dept_name, "teaser": str, "url": telegraph_url}, ...] —
    only departments that produced an article.
    Returns (html_text, inline_keyboard) where inline_keyboard is Telegram's
    list-of-rows of {"text","url"} buttons (one row per department).
    """
    title = "📊 Головне за день" if lang == "ua" else "📊 Daily briefing"
    read_more = "Детальніше" if lang == "ua" else "Read more"
    lines = [f"<b>{escape_html(title)}</b>", f"<i>{escape_html(date_str)}</i>", ""]
    keyboard: list = []
    for it in items:
        name = escape_html(it.get("name", ""))
        teaser = escape_html((it.get("teaser") or "").strip())
        lines.append(f"📌 <b>{name}</b>")
        if teaser:
            lines.append(teaser)
        lines.append("")
        url = it.get("url")
        if url:
            keyboard.append([{"text": f"{it.get('name','')} — {read_more} →", "url": url}])
    text = "\n".join(lines).strip()
    return text, keyboard


def telegram_chunks(text: str, limit: int = TELEGRAM_MSG_LIMIT) -> list[str]:
    """Split a message so each chunk is <= limit, breaking on line boundaries
    (and, if a single line is too long, on spaces / hard character cuts)."""
    if text is None:
        return []
    text = text.strip()
    if not text:
        return []
    if len(text) <= limit:
        return [text]

    target = min(_CHUNK_TARGET, limit)
    chunks: list[str] = []
    current = ""
    for line in text.split("\n"):
        # A single oversized line: flush current, then hard-split the line.
        if len(line) > target:
            if current:
                chunks.append(current.rstrip("\n"))
                current = ""
            chunks.extend(_split_long_line(line, target))
            continue
        candidate = f"{current}{line}\n"
        if len(candidate) > target and current:
            chunks.append(current.rstrip("\n"))
            current = f"{line}\n"
        else:
            current = candidate
    if current.strip():
        chunks.append(current.rstrip("\n"))
    return chunks


def _split_long_line(line: str, target: int) -> list[str]:
    out: list[str] = []
    words = line.split(" ")
    cur = ""
    for w in words:
        if len(w) > target:  # single monster token — hard cut
            if cur:
                out.append(cur)
                cur = ""
            for k in range(0, len(w), target):
                out.append(w[k:k + target])
            continue
        cand = f"{cur} {w}".strip()
        if len(cand) > target and cur:
            out.append(cur)
            cur = w
        else:
            cur = cand
    if cur:
        out.append(cur)
    return out


def format_article_html(dept_name: str, body: str, date_str: str,
                        sources: list[tuple[str, str]] | None = None,
                        max_sources: int = 5) -> str:
    """Assemble the final Telegram HTML message for one department.

    sources: list of (title, url) tuples appended as a compact source list.
    """
    header = f"📊 <b>{escape_html(dept_name)}</b>\n<i>{escape_html(date_str)}</i>\n\n"
    out = header + escape_html(body).strip()
    if sources:
        seen = set()
        lines = ["\n\n<b>Джерела:</b>"]
        n = 0
        for title, url in sources:
            if not url or url in seen:
                continue
            seen.add(url)
            n += 1
            label = escape_html((title or url)[:80])
            lines.append(f'• <a href="{escape_html(url)}">{label}</a>')
            if n >= max_sources:
                break
        if n:
            out += "\n".join(lines)
    return out


def collect_sources(facts: list[dict], limit: int = 5) -> list[tuple[str, str]]:
    """Pull (title, link) source pairs from a department's facts, de-duplicated."""
    out: list[tuple[str, str]] = []
    seen: set[str] = set()
    for f in facts:
        url = (f.get("link") or f.get("source_url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        title = (f.get("title") or f.get("source_publisher") or "").strip()
        out.append((title, url))
        if len(out) >= limit:
            break
    return out
