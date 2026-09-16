"""HTML scrapers for Ukrainian government sources that have no RSS feed.

dls.gov.ua (Держлікслужба) and kmu.gov.ua (Кабінет Міністрів, НПА) are
server-rendered HTML, so BeautifulSoup can parse them directly. Each scraper
returns a *feedparser-like* object (``.entries`` of objects with ``title``,
``link``, ``published`` (RFC822), ``summary``) so fetch_and_store_news can treat
them exactly like an RSS feed.

The pure ``parse_*_html`` functions are unit-tested against sample markup that
mirrors the live structure. If a site changes its markup, adjust the selectors
here — the async wrappers already fail soft (return no entries) so a broken
scraper never crashes the news loop.
"""

from __future__ import annotations

import datetime
import email.utils
import logging
import re
from types import SimpleNamespace
from urllib.parse import urljoin

import httpx
from bs4 import BeautifulSoup

logger = logging.getLogger("macroharvey")

_UA = "Mozilla/5.0 (compatible; MacroHarveyBot/1.0; +https://alliance-news)"
_DATE_RE = re.compile(r"(\d{2})\.(\d{2})\.(\d{4})")


def _to_rfc822(date_str: str) -> str:
    """'15.09.2026' -> RFC822 string (UTC). '' if it can't be parsed."""
    m = _DATE_RE.search(date_str or "")
    if not m:
        return ""
    try:
        d, mo, y = (int(x) for x in m.groups())
        dt = datetime.datetime(y, mo, d, tzinfo=datetime.timezone.utc)
        return email.utils.format_datetime(dt)
    except ValueError:
        return ""


def _shim(items: list[dict]) -> SimpleNamespace:
    """Wrap parsed items into a feedparser-like object."""
    return SimpleNamespace(entries=[
        SimpleNamespace(
            title=i["title"], link=i["link"],
            published=i.get("published", ""), summary=i.get("title", ""),
        )
        for i in items
    ])


def parse_dls_html(html: str, base: str = "https://www.dls.gov.ua",
                   limit: int = 25) -> list[dict]:
    """Parse the Держлікслужба 'for_subject' listing.

    Items look like: <a href=".../for_subject/<slug>/">
                       <span class="date">DD.MM.YYYY</span>
                       <span class="title">…</span></a>
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/for_subject/" not in href:
            continue
        link = urljoin(base, href).split("?")[0]
        norm = link.rstrip("/")
        if norm.endswith("/for_subject"):  # section root / pagination, not an item
            continue
        if link in seen:
            continue
        title_el = a.find(class_="title")
        date_el = a.find(class_="date")
        if title_el:
            title = title_el.get_text(" ", strip=True)
        else:
            title = a.get_text(" ", strip=True)
            if date_el:
                title = title.replace(date_el.get_text(" ", strip=True), "").strip()
        if not title or len(title) < 8:
            continue
        seen.add(link)
        published = _to_rfc822(date_el.get_text(strip=True)) if date_el else ""
        out.append({"title": title[:300], "link": link, "published": published})
        if len(out) >= limit:
            break
    return out


def parse_kmu_html(html: str, base: str = "https://www.kmu.gov.ua",
                   limit: int = 25) -> list[dict]:
    """Parse the Кабінет Міністрів НПА listing.

    Items are <a href="/npas/<slug>">Title</a> with a nearby
    'Постанова від DD.MM.YYYY № N' date string.
    """
    soup = BeautifulSoup(html, "html.parser")
    out: list[dict] = []
    seen: set[str] = set()
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "/npas/" not in href:
            continue
        link = urljoin(base, href).split("?")[0]
        if link in seen:
            continue
        title = a.get_text(" ", strip=True)
        if not title or len(title) < 8:
            continue
        parent = a.find_parent()
        ctext = parent.get_text(" ", strip=True) if parent else title
        published = _to_rfc822(ctext)
        seen.add(link)
        out.append({"title": title[:300], "link": link, "published": published})
        if len(out) >= limit:
            break
    return out


async def _fetch_html(url: str) -> str:
    async with httpx.AsyncClient(timeout=25, follow_redirects=True,
                                 headers={"User-Agent": _UA}) as client:
        r = await client.get(url)
        r.raise_for_status()
        return r.text


async def scrape_dls(url: str) -> SimpleNamespace:
    try:
        html = await _fetch_html(url)
        items = parse_dls_html(html)
        logger.info("scrape_dls: %d items from %s", len(items), url)
        return _shim(items)
    except Exception as e:
        logger.warning("scrape_dls failed for %s: %s", url, e)
        return _shim([])


async def scrape_kmu(url: str) -> SimpleNamespace:
    try:
        html = await _fetch_html(url)
        items = parse_kmu_html(html)
        logger.info("scrape_kmu: %d items from %s", len(items), url)
        return _shim(items)
    except Exception as e:
        logger.warning("scrape_kmu failed for %s: %s", url, e)
        return _shim([])
