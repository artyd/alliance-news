import datetime

from app import corp_tracking as ct

HEADER = ("№ листа,,Товар,Кто везет,Кол-во,Морская линия,№ контейнер,№ ТТН,Дата прибытия планируемая,"
          "Место прибытия,Дата выхода,Место выхода,Морская Линия,Комментарий,Место растаможки,Склад выгрузки\n")
TODAY = datetime.date(2026, 10, 9)


def rows(*lines):
    return HEADER + "\n".join(lines) + "\n"


def test_container_in_agent_column_and_sea_mode():
    csv_text = rows(',48156,Сорбитол,MAGU2286053,25 т,MSC,,,20.10.2026,Гданск,01.09.2026,Шанхай,'
                    'https://www.msc.com/track,,,')
    [i] = ct.parse_sheet(csv_text, TODAY)
    assert i["container"] == "MAGU2286053" and i["number"] == "MAGU2286053"
    assert i["agent"] == ""                     # it was a container number, not a forwarder
    assert i["mode"] == "sea" and i["stage"] == "transit"
    assert i["eta"] == "2026-10-20" and i["origin"] == "Шанхай" and i["dest"] == "Гданск"


def test_mangled_number_is_dropped_and_air_detected():
    csv_text = rows(',,Капсулы,,,,,"1,42551E+11",13.10.2026,,01.10.2026,,https://ct.shipmentlink.com/x,,,',
                    ',,Атропин,,,,,AWB 020-45508923,12.10.2026,Варшава,05.10.2026,,https://www.lufthansa-cargo.com/x,,,')
    caps, atr = ct.parse_sheet(csv_text, TODAY)
    assert caps["number"] == ""                 # "1,42551E+11" is Google's number format, not a TTN
    assert atr["number"] == "020-45508923" and atr["mode"] == "air"


def test_stages_and_filtering():
    csv_text = rows(
        ',,Старий,,,,,,10.01.2025,,01.01.2025,,,растаможен 23/04/2025,,',     # done long ago → hidden
        ',,Свіжий розмитнений,,,,,,01.10.2026,,01.09.2026,,,растаможен 03/10,,',  # done recently → shown
        ',,Прибув,,,,MSCU1234567,,05.10.2026,,01.09.2026,,,Import Discharged,,',
        ',,DHL,,,,,23 1258 4330,08.10.2026,,01.10.2026,,https://www.dhl.com/x,"\t\r\nDLV\r\nWAW",,',
        ',,Забутий,,,,,,01.01.2026,,01.12.2025,,,,,',                          # ETA >45 days ago, never closed
        ',,Трохи запізнюється,,,,,,20.09.2026,,01.08.2026,,,,,',                # 19 days late → still shown
    )
    items = {i["product"]: i for i in ct.parse_sheet(csv_text, TODAY)}
    assert set(items) == {"Свіжий розмитнений", "Прибув", "DHL", "Трохи запізнюється"}
    assert items["Свіжий розмитнений"]["stage"] == "done"
    assert items["Прибув"]["stage"] == "arrived"
    assert items["DHL"]["stage"] == "done"      # "DLV" after a line break


def test_undated_rows_only_from_recent_part_of_sheet():
    old = [f',,Старий {n},,,,,,,,,,,,,' for n in range(5)]
    new = [',,Новий без дат,,,,,,,,,,,,,']
    items = ct.parse_sheet(rows(*old, *new), TODAY, recent_rows=1)
    assert [i["product"] for i in items] == ["Новий без дат"]


def test_keys_are_stable_and_unique():
    csv_text = rows(',,А,,,,,111111111,01.11.2026,,01.10.2026,,,,,', ',,Б,,,,,222222222,01.11.2026,,01.10.2026,,,,,')
    a1 = [i["key"] for i in ct.parse_sheet(csv_text, TODAY)]
    a2 = [i["key"] for i in ct.parse_sheet(rows(',,Новий,,,,,,,,,,,,,', ',,А,,,,,111111111,01.11.2026,,01.10.2026,,,,,',
                                                ',,Б,,,,,222222222,01.11.2026,,01.10.2026,,,,,'), TODAY)]
    assert len(set(a1)) == 2 and set(a1) <= set(a2)   # inserting a row doesn't change other keys


def test_stats():
    csv_text = rows(',,A,,,,,,12.10.2026,,01.10.2026,,,,,',              # this week
                    ',,B,,,,,,01.10.2026,,01.09.2026,,,,,',              # late
                    ',,C,,,,MSCU1234567,,01.10.2026,,01.09.2026,,,Discharged,,')  # arrived, not late
    st = ct.stats(ct.parse_sheet(csv_text, TODAY), TODAY)
    assert st["active"] == 3 and st["transit"] == 2 and st["arrived"] == 1
    assert st["week"] == 1 and st["late"] == 1
