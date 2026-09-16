"""Unit tests for the gov-site HTML parsers (pure, no network).

Sample markup mirrors the live structure observed on dls.gov.ua / kmu.gov.ua.
Run:  python tests/test_scrapers.py
"""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.scrapers import parse_dls_html, parse_kmu_html, _to_rfc822

DLS_HTML = """
<html><body>
<div class="news-list">
  <a href="/for_subject/zasidannia-robochoi-hrupy-180926/">
     <span class="date">15.09.2026</span>
     <span class="title">Засідання робочої групи відбудеться 18 вересня</span>
  </a>
  <a href="https://www.dls.gov.ua/for_subject/roziasnennia-subiektam/">
     <span class="date">14.09.2026</span>
     <span class="title">Роз'яснення для суб'єктів господарювання</span>
  </a>
  <a href="/for_subject/">Всі новини</a>            <!-- section root, must be skipped -->
  <a href="/about/">Про нас</a>                     <!-- unrelated, must be skipped -->
</div>
</body></html>
"""

KMU_HTML = """
<html><body>
<div class="npa-list">
  <div class="item">
    <a href="/npas/pro-zatverdzhennia-poriadku-560-160524">Про затвердження Порядку проведення</a>
    <span>Постанова від 16.05.2024 № 560</span>
  </div>
  <div class="item">
    <a href="/npas/deiaki-pytannia-realizatsii-76-270123">Деякі питання реалізації положень</a>
    <span>Постанова від 27.01.2023 № 76</span>
  </div>
  <a href="/about">Контакти</a>                      <!-- not /npas/, skipped -->
</div>
</body></html>
"""


def test_to_rfc822():
    out = _to_rfc822("15.09.2026")
    assert "15 Sep 2026" in out
    assert _to_rfc822("garbage") == ""
    assert _to_rfc822("Постанова від 16.05.2024 № 560") != ""  # finds date in text


def test_parse_dls_basic():
    items = parse_dls_html(DLS_HTML)
    assert len(items) == 2
    assert items[0]["title"].startswith("Засідання робочої групи")
    assert items[0]["link"] == "https://www.dls.gov.ua/for_subject/zasidannia-robochoi-hrupy-180926/"
    assert "15 Sep 2026" in items[0]["published"]


def test_parse_dls_skips_root_and_unrelated():
    items = parse_dls_html(DLS_HTML)
    links = [i["link"] for i in items]
    assert not any(l.rstrip("/").endswith("/for_subject") for l in links)
    assert not any("/about/" in l for l in links)


def test_parse_dls_dedup():
    html = DLS_HTML + DLS_HTML
    items = parse_dls_html(html)
    assert len(items) == 2  # duplicates collapsed by link


def test_parse_kmu_basic():
    items = parse_kmu_html(KMU_HTML)
    assert len(items) == 2
    assert items[0]["link"] == "https://www.kmu.gov.ua/npas/pro-zatverdzhennia-poriadku-560-160524"
    assert items[0]["title"].startswith("Про затвердження")
    assert "16 May 2024" in items[0]["published"]


def test_parse_kmu_skips_non_npas():
    items = parse_kmu_html(KMU_HTML)
    assert all("/npas/" in i["link"] for i in items)


def test_limit_respected():
    assert len(parse_dls_html(DLS_HTML, limit=1)) == 1


if __name__ == "__main__":
    passed = 0
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
            print(f"  PASS {name}")
            passed += 1
    print(f"\n{passed} tests passed")
