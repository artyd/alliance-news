# MacroHarvey — DESIGN FREEZE (Mini App)

> **НЕЗМІННИЙ ЕТАЛОН.** Функціонал може змінюватись. Дизайн, кольорова схема, типографіка, анімації, структура навігації і стиль компонентів — **НЕЗМІННІ** без явної команди власника.

## Концепція

- **Стиль:** Чорно-білий фінансовий термінал. Мінімалізм.
- **Тема за замовчуванням:** DARK (`#000000` фон)
- **Допоміжна тема:** LIGHT (`#F5F5F5`, перемикається кнопкою)
- **Акцентні кольори:** `#22C55E` зелений (дії, активні стани) та `#EF4444` червоний (падіння, помилки)
- **Шрифт:** `-apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif` (системний)

## CSS змінні

### Dark mode (`:root` — за замовчуванням)
```css
--bg:      #000000   /* чисто чорний */
--surface: #0F0F0F   /* картки, панелі */
--surface2:#1A1A1A   /* hover/active */
--border:  #2A2A2A   /* всі рамки */
--text:    #FFFFFF   /* основний текст */
--sub:     #8A8A8A   /* другорядний */
--muted:   #555555   /* заглушений */
--green:   #22C55E
--red:     #EF4444
--shadow:  0 2px 16px rgba(0,0,0,.9)
--r:       13px
```

### Light mode (`[data-light]`)
```css
--bg:      #F5F5F5
--surface: #FFFFFF
--surface2:#EBEBEB
--border:  #DDDDDD
--text:    #0A0A0A
--sub:     #666666
--muted:   #AAAAAA
--shadow:  0 2px 10px rgba(0,0,0,.08)
```

## Кольори категорій (бейджі — НЕЗМІННІ)

| Категорія | HEX | Емодзі |
|---|---|---|
| all | `#4B5563` | 📋 |
| api | `#2563EB` | 💊 |
| cosmetic | `#DB2777` | 🧴 |
| herbal | `#16A34A` | 🌿 |
| veterinary | `#7C3AED` | 🐾 |
| food | `#B45309` | 🌾 |
| feed | `#92400E` | 🐄 |
| capsules | `#0E7490` | 🔬 |
| pvc | `#4338CA` | 📦 |
| logistics | `#B91C1C` | 🚢 |
| global_sources | `#374151` | 🌐 |
| good_news | `#059669` | ✨ |
| market_alerts | `#C2410C` | ⚡ |

## Header

```css
header { padding:12px 16px; background:var(--bg); border-bottom:1px solid var(--border); }
/* Логотип: 28×28px, object-fit:contain, без фону/рамки */
/* Заголовок: font-size:15px; font-weight:800; letter-spacing:-.2px */
/* Кнопки: background:var(--surface); border:1px solid var(--border); border-radius:9px; min-width:36px; height:32px */
```

## Bottom Navigation

```css
nav button { flex:1; padding:8px 2px 9px; font-size:10px; font-weight:500; color:var(--sub); }
nav button .ico { font-size:25px; }
nav button.on { color:var(--text); }
/* Активний індикатор — ЛІНІЯ ЗНИЗУ */
nav button.on::after {
  content:''; position:absolute; bottom:0; left:50%;
  transform:translateX(-50%); width:20px; height:2px;
  border-radius:1px 1px 0 0; background:var(--text);
}
```

На desktop (≥768px): sidebar 220px зліва, активний індикатор — вертикальна зелена смуга 3px.

## Splash Screen

```css
#splash { position:fixed; inset:0; background:var(--bg); z-index:9999; transition:opacity .5s; }
.sp-logo-img { width:72vw; max-width:360px; animation:logo-pulse 2s ease-in-out infinite; }
@keyframes logo-pulse { 0%,100%{transform:scale(1);opacity:1} 50%{transform:scale(1.06);opacity:.85} }
```
- Логотип 72% ширини. Тільки пульсація. Авто-hide через **1200 мс**.

## Картки новин

```css
.ncard { background:var(--surface); border-radius:13px; border:1px solid var(--border); }
.ntitle { font-size:13.5px; font-weight:700; line-height:1.45; }
.nsumm { font-size:12.5px; color:var(--sub); -webkit-line-clamp:2; overflow:hidden; }
.nbadge { padding:3px 9px; border-radius:5px; font-size:11px; font-weight:700; color:#fff; }
.ntime { font-size:11px; color:var(--muted); }
```

## Чіпи фільтрації

```css
.chip { padding:6px 13px; border-radius:20px; font-size:12px; font-weight:600; border:1.5px solid var(--border); }
.chip.on { border-color:var(--text); color:var(--text); background:var(--surface2); }
```

## Картки звітів

```css
.rep-card { background:var(--surface); border-radius:13px; border:1px solid var(--border); padding:14px 15px; }
.rep-ico { font-size:26px; }
.rep-type { font-size:13px; font-weight:700; }
.rep-date { font-size:11.5px; color:var(--sub); }
```

## Ринкові картки

```css
.pcard { background:var(--surface); border:1px solid var(--border); border-radius:13px; padding:13px; cursor:pointer; }
.pcval { font-size:17px; font-weight:800; letter-spacing:-.5px; }
.pcchg.up { color:#22C55E; } .pcchg.dn { color:#EF4444; }
```

Графік деталь-виду: `borderWidth:2`, `tension:.35`, `pointRadius:0`, gradient fill. Фон: `var(--bg)`.

## Трекінг компоненти

```css
/* Режим-кнопки */
.trk-mode-btn { padding:22px 8px; border-radius:13px; font-size:13px; font-weight:700; }
.trk-mode-btn.on { border-color:var(--green); }
/* Input */
.trk-input { padding:13px 16px; border-radius:13px; font-size:15px; }
.trk-input:focus { border-color:var(--green); }
/* Кнопка "Знайти" */
.trk-go { background:var(--green); color:#000; font-size:15px; font-weight:800; border-radius:13px; }
/* Radar */
@keyframes trk-ping { 0%{transform:scale(.3);opacity:.9} 100%{transform:scale(2.6);opacity:0} }
/* Timeline dots */
.trk-dot.done { background:#22C55E22; border:2px solid #22C55E; }
.trk-dot.active { background:#22C55E; border:2px solid #22C55E; }
```

## Skeleton анімація

```css
.sk { background:linear-gradient(90deg, var(--surface) 25%, var(--surface2) 50%, var(--surface) 75%);
      background-size:200% 100%; animation:shimmer 1.5s infinite; border-radius:13px; }
@keyframes shimmer { 0%{background-position:200% 0} 100%{background-position:-200% 0} }
```

## 10 правил незмінності

1. Кольори: `#000000` фон, `#0F0F0F` картки, `#2A2A2A` рамки, `#22C55E` зелений, `#EF4444` червоний
2. Border-radius: `13px` картки, `20px` пілюлі, `9px` header-кнопки, `5px` бейджі
3. Типографіка: заголовок картки `13.5px/700`, саммарі `12.5px`, бейдж `11px/700`, нав `10px/500`, іконки нав `25px`
4. Splash: логотип 72vw + pulse. Без тексту, кілець, затримок > 1.2 сек
5. Навігація: рівно 5 вкладок (📰/📋/➕/📈/📡), порядок не змінювати
6. Зелений — тільки для дій та активних станів. Не для декору
7. Анімації — тільки: logo-pulse, shimmer, trk-ping, hover 0.15s
8. Картки новин: бейдж → заголовок → 2-рядкове саммарі → [час + кнопка]. Без зображень у картках
9. UI тексти: головна мова UA, оновлювати ВСІ 3 мови в об'єкті `UI`
10. Self-contained: весь HTML у `_WEBAPP_HTML` в `main.py`
