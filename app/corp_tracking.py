"""«Вантажі компанії» — the company's shipments from the shared Google Sheet.

The logistics team keeps one sheet (one row per shipment). Every hour the bot
downloads it as CSV, normalises the rows here, stores them and refreshes the
live carrier status for active ones. All Mini App users see the same list.

The sheet is filled by hand, so parsing is defensive: columns are found by
header name, a container number may sit in the "Кто везет" column, numbers can
be mangled by Google's number formatting ("1,42551E+11"), statuses are free
text in "Комментарий".
"""

from __future__ import annotations

import csv
import datetime as _dt
import hashlib
import io
import re

DEFAULT_SHEET_CSV = ("https://docs.google.com/spreadsheets/d/1LlnnekYqb_6RzEK0yZsWC4_sMIV8QjGL8AUNuL3iBTg"
                     "/export?format=csv&gid=1401749917")

# header text (lower-case, startswith) → field
_HEADERS = [
    ("№ листа", "sheet_no"), ("товар", "product"), ("кто везет", "agent"), ("кол-во", "qty"),
    ("морская линия", "line"), ("№ контейнер", "container"), ("№ ттн", "ttn"),
    ("дата прибытия", "eta"), ("место прибытия", "dest"), ("дата выхода", "departed"),
    ("место выхода", "origin"), ("комментарий", "comment"), ("место растаможки", "customs"),
    ("склад выгрузки", "warehouse"),
]
_CONTAINER_RE = re.compile(r"\b([A-Z]{4}\s?\d{7})\b")
# finished: cleared / handed over / at the client
_DONE_WORDS = ("растаможен", "растаможено", "доставлен", "у клиента", "в офисе", "delivered",
               "вручен", "отримано", "получено", "pick-up by merchant", "в бц", "клиент получил",
               "видано", "на заводе клиента", "dlv ", "dlv\t", "доставка мистом в офис")
# physically arrived (port / airport / branch) but not finished yet
_ARRIVED_WORDS = ("discharged", "discharge", "vessel arrived", "vessel arrival", "arrived", "прибыл",
                  "прибув", "gate out", "готове до видачі", "у айкарго", "забрали", "import discharged",
                  "rcf", "received")

AIR_DOMAINS = ("lufthansa-cargo", "emirates", "turkishcargo", "airchinacargo", "siacargo", "qatarcargo",
               "cargo.lot", "afklcargo", "iagcargo", "etihadcargo")
PARCEL_DOMAINS = ("dhl.", "fedex.", "meest", "novaposhta", "ups.com", "ems.", "ukrposhta")
SEA_DOMAINS = ("msc.com", "maersk", "cma-cgm", "coscoshipping", "hapag-lloyd", "shipmentlink", "searates",
               "yangming", "one-line", "zim.com", "hmm21", "evergreen")


def _clean(v) -> str:
    return re.sub(r"[ \t]+", " ", (v or "").replace("\r", "")).strip()


def _parse_date(v: str) -> _dt.date | None:
    m = re.search(r"(\d{1,2})[./](\d{1,2})[./](\d{2,4})", v or "")
    if not m:
        return None
    d, mo, y = (int(x) for x in m.groups())
    if y < 100:
        y += 2000
    try:
        return _dt.date(y, mo, d)
    except ValueError:
        return None


def _usable_number(v: str) -> str:
    """Tracking number from a cell, or '' when the cell is not one (mangled by
    the sheet's number format, a carrier name, a note…)."""
    v = _clean(v)
    if not v or re.search(r"\d,\d+E\+\d+", v, re.I):
        return ""
    v = re.sub(r"(?i)^(no\.?:?|awb|booking\s*no\s*:?|bk\s*no\.?:?)\s*", "", v).strip()
    compact = re.sub(r"[\s-]", "", v)
    if len(compact) < 6 or not re.search(r"\d{4,}", compact):
        return ""
    return v


def _mode(url: str, number: str, container: str) -> str:
    u = (url or "").lower()
    if container or any(d in u for d in SEA_DOMAINS):
        return "sea"
    if any(d in u for d in AIR_DOMAINS) or re.fullmatch(r"\d{3}-?\d{8}", number.replace(" ", "")):
        return "air"
    if any(d in u for d in PARCEL_DOMAINS) or re.fullmatch(r"[A-Z]{2}\d{9,}[A-Z0-9]*", number.replace(" ", "")):
        return "parcel"
    return "other"


def is_done(status: str) -> bool:
    return stage_of(status) == "done"


def stage_of(*texts: str) -> str:
    """'done' | 'arrived' | 'transit' from the free-text status cells."""
    s = re.sub(r"\s+", " ", " ".join(t or "" for t in texts)).lower() + " "
    if any(w in s for w in _DONE_WORDS):
        return "done"
    if any(w in s for w in _ARRIVED_WORDS):
        return "arrived"
    return "transit"


def parse_sheet(csv_text: str, today: _dt.date | None = None, keep_done_days: int = 30,
                recent_rows: int = 70) -> list[dict]:
    """CSV text → shipments worth showing:
    * in transit / arrived — always (unless the dates are >180 days old);
    * finished — only within `keep_done_days` of their ETA/departure;
    * rows with no dates at all — only from the last `recent_rows` rows of the
      sheet (it is chronological; older undated rows are long closed)."""
    today = today or _dt.date.today()
    rows = list(csv.reader(io.StringIO(csv_text)))
    if not rows:
        return []
    filled = [i for i, r in enumerate(rows) if any(_clean(c) for c in r)]
    recent_from = (filled[-1] - recent_rows + 1) if filled else 0
    header = [_clean(h).lower() for h in rows[0]]
    col: dict[str, int] = {}
    for i, h in enumerate(header):
        for prefix, field in _HEADERS:
            if h.startswith(prefix) and field not in col:
                col[field] = i
                break
    # the tracking-link column has no header of its own: the cell right after
    # "Место выхода" ("Морская Линия" in the sheet) holds URLs
    url_col = next((i for i, h in enumerate(header) if i > col.get("origin", 99) and "линия" in h), None)

    out: list[dict] = []
    for idx, r in enumerate(rows[1:], start=1):
        g = lambda f: _clean(r[col[f]]) if f in col and col[f] < len(r) else ""
        product = g("product")
        if not product:
            continue
        url = _clean(r[url_col]) if url_col is not None and url_col < len(r) else ""
        url = url if url.startswith("http") else ""
        cells = " ".join([g("agent"), g("container"), g("ttn"), g("line")])
        m = _CONTAINER_RE.search(cells.upper())
        container = m.group(1).replace(" ", "") if m else ""
        agent = g("agent") if not _CONTAINER_RE.fullmatch(g("agent").upper()) else ""
        number = container or _usable_number(g("ttn")) or _usable_number(g("container"))
        comment = g("comment")
        eta, departed = _parse_date(g("eta")), _parse_date(g("departed"))
        stage = stage_of(comment, g("customs"), g("warehouse"))
        done = stage == "done"
        ref = eta or departed
        if ref is None and idx < recent_from:
            continue                      # old undated row
        if done and (ref is None or (today - ref).days > keep_done_days):
            continue
        if not done and eta is not None and (today - eta).days > 45:
            continue                      # ETA long gone — a row nobody closed
        if not done and eta is None and departed is not None and (today - departed).days > 120:
            continue
        if stage == "arrived" and ref is not None and (today - ref).days > 60:
            continue                      # arrived long ago, just never marked done
        key_src = "|".join([product.lower(), number, g("departed"), g("sheet_no")])
        out.append({
            "key": hashlib.sha1(key_src.encode("utf-8")).hexdigest()[:16],
            "sheet_no": g("sheet_no"), "product": product, "agent": agent, "qty": g("qty"),
            "line": g("line"), "container": container, "number": number,
            "mode": _mode(url, number, container),
            "eta": eta.isoformat() if eta else None, "departed": departed.isoformat() if departed else None,
            "origin": g("origin"), "dest": g("dest"), "tracking_url": url,
            "comment": comment, "customs": g("customs"), "warehouse": g("warehouse"),
            "stage": stage, "done": done, "row": idx + 1,
        })
    return out


def stats(items: list[dict], today: _dt.date | None = None) -> dict:
    """Counters for the header tiles."""
    today = today or _dt.date.today()
    active = [i for i in items if not i["done"]]
    def eta(i):
        return _dt.date.fromisoformat(i["eta"]) if i.get("eta") else None
    transit = [i for i in active if i.get("stage") != "arrived"]
    soon = [i for i in transit if eta(i) and 0 <= (eta(i) - today).days <= 7]
    late = [i for i in transit if eta(i) and (eta(i) - today).days < 0]
    return {"active": len(active), "transit": len(transit),
            "arrived": len(active) - len(transit), "week": len(soon), "late": len(late),
            "done": len(items) - len(active),
            "sea": sum(1 for i in active if i["mode"] == "sea"),
            "air": sum(1 for i in active if i["mode"] == "air"),
            "parcel": sum(1 for i in active if i["mode"] == "parcel")}


# ── Notifications ────────────────────────────────────────────────────────────
STAGE_RANK = {"transit": 0, "arrived": 1, "done": 2}


def shipment_events(prev: dict | None, cur: dict, today: _dt.date | None = None) -> list[tuple[str, str]]:
    """What changed for one shipment since we last notified about it.
    prev: {"stage", "eta", "late_notified"} as stored after the last
    notification (None = never seen). Returns [(kind, detail)] with kinds
    new / arrived / done / eta / late."""
    today = today or _dt.date.today()
    out: list[tuple[str, str]] = []
    stage, eta = cur.get("stage", "transit"), cur.get("eta")
    if prev is None:
        ref = cur.get("eta") or cur.get("departed")
        fresh = ref and abs((today - _dt.date.fromisoformat(ref)).days) <= 30
        if stage == "transit" and fresh:
            out.append(("new", ""))
        return out
    if STAGE_RANK.get(stage, 0) > STAGE_RANK.get(prev.get("stage") or "transit", 0):
        out.append((stage, ""))
    elif stage == "transit" and eta and prev.get("eta") and eta != prev["eta"]:
        delta = (_dt.date.fromisoformat(eta) - _dt.date.fromisoformat(prev["eta"])).days
        if abs(delta) >= 2:
            out.append(("eta", f"{prev['eta']}|{eta}|{delta}"))
    if (stage == "transit" and eta and _dt.date.fromisoformat(eta) < today
            and not prev.get("late_notified") and not any(k == "eta" for k, _ in out)):
        out.append(("late", str((today - _dt.date.fromisoformat(eta)).days)))
    return out


# Cities that appear in the sheet (Russian spelling) and in strike reports
# (Ukrainian / English) → one canonical name.
CITY_ALIASES = {      # word stems: match any case form (Одеса / Одесі / Одесса / Одещина)
    "Київ": ["київ", "києв", "киев", "kyiv", "kiev"],
    "Одеса": ["одес", "одещ", "odes"],
    "Чорноморськ": ["чорноморськ", "черноморск", "chornomorsk"],
    "Південний": ["південн", "южн", "pivdenn"],
    "Ізмаїл": ["ізмаїл", "измаил", "izmail"],
    "Рені": ["рені", "рени", "reni"],
    "Харків": ["харків", "харков", "харьков", "kharkiv"],
    "Дніпро": ["дніпр", "днепр", "dnipr"],
    "Львів": ["львів", "львов", "lviv"],
    "Луцьк": ["луцьк", "луцк", "lutsk"],
    "Тернопіль": ["тернопіл", "тернопол", "ternopil"],
    "Лубни": ["лубн", "lubny"],
    "Біла Церква": ["біла церкв", "білій церкв", "белая церков", "bila tserkv"],
    "Бориспіль": ["бориспіл", "борисп", "boryspil"],
    "Бровари": ["бровар", "brovary"],
    "Вінниця": ["вінниц", "винниц", "vinnyts"],
    "Полтава": ["полтав", "poltav"],
    "Запоріжжя": ["запоріж", "запорож", "zaporizh"],
    "Миколаїв": ["миколаїв", "миколаєв", "николаев", "mykolaiv"],
}
# Clients the team writes as a suffix of the product ("Мометазон Лубны")
CLIENT_MARKERS = {"лубны": "Лубнифарм", "лубни": "Лубнифарм", "бхфз": "Борщагівський ХФЗ",
                  "кмп": "Київмедпрепарат", "фармак": "Фармак", "дарница": "Дарниця", "дарниця": "Дарниця",
                  "артериум": "Артеріум", "здоровье": "Здоров'я (Харків)", "юрия": "Юрія-Фарм",
                  "биофарма": "Біофарма", "технолог": "Технолог (Умань)"}


def cities_in(text: str) -> set[str]:
    t = (text or "").lower().replace("ʼ", "'").replace("’", "'")
    return {city for city, al in CITY_ALIASES.items()
            if any(re.search(r"(?<![а-яіїєґa-z])" + re.escape(a), t) for a in al)}


def clients_in(text: str) -> set[str]:
    t = (text or "").lower()
    return {name for marker, name in CLIENT_MARKERS.items() if re.search(r"(?<![а-яіїєґa-z])" + marker + r"(?![а-яіїєґa-z])", t)}


def strike_hits(strike_text: str, strike_company: str, shipments: list[dict]) -> list[tuple[dict, str]]:
    """Active shipments a strike may affect: same city as the shipment's
    destination / customs / warehouse, or the struck company is the client
    the shipment is for. Returns [(shipment, reason)]."""
    s_cities = cities_in(strike_text)
    s_company = (strike_company or "").lower()
    out = []
    for it in shipments:
        if it.get("done"):
            continue
        where = " ".join([it.get("dest", ""), it.get("customs", ""), it.get("warehouse", "")])
        common = s_cities & cities_in(where)
        clients = clients_in(it.get("product", ""))
        client_hit = next((c for c in clients if c.lower().split(" ")[0][:6] in s_company), None)
        if client_hit:
            out.append((it, f"client:{client_hit}"))
        elif common:
            out.append((it, f"city:{sorted(common)[0]}"))
    return out
