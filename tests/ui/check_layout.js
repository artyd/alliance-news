() => {
  const vw = document.documentElement.clientWidth;
  const issues = [];
  const scrollers = '.chips,.hscroll,.trk-filter-row,.weather-popular,.wx-chips,.wx-hours,.mk-modal-list,.res,.body,.list';
  const cont = '.glass,.card,.tile,.pcard,.stat,.dept,.hero,.menu,.mi,.tb-row,.msg,.trk-sv-card,.rep-card,.wh-card,.curr-card,.note,.hit,.sheet,.composer,.tabbar';
  const visible = el => { const cs = getComputedStyle(el); if(cs.visibility==='hidden' || cs.display==='none' || +cs.opacity===0) return false;
    const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0; };
  const label = el => { let s = el.tagName.toLowerCase(); if(el.id) s += '#'+el.id; if(el.className && typeof el.className==='string') s += '.'+el.className.trim().split(/\s+/).slice(0,2).join('.');
    const t = (el.textContent||'').trim().replace(/\s+/g,' ').slice(0,40); return s + (t ? ` «${t}»` : ''); };
  const roots = [...document.querySelectorAll('.view.on, .sheet.on, .overlay.on, .mk-modal-ov.on, .tabbar, #orb')];
  const seen = new Set();
  for(const root of roots){
    for(const el of [root, ...root.querySelectorAll('*')]){
      if(!visible(el) || el.closest('svg') && el.tagName.toLowerCase()!=='svg') continue;
      const r = el.getBoundingClientRect();
      const inScroller = el.parentElement && el.parentElement.closest(scrollers) && el.parentElement.closest(scrollers) !== el;
      const sc = el.parentElement?.closest('.chips,.hscroll,.trk-filter-row,.weather-popular,.wx-chips,.wx-hours');
      if(!sc && (r.right > vw + 1 || r.left < -1)) issues.push({kind:'viewport', el: label(el), left: Math.round(r.left), right: Math.round(r.right), vw});
      const box = el.parentElement && el.parentElement.closest(cont);
      if(box && !sc){
        const b = box.getBoundingClientRect();
        if(r.right > b.right + 1.5 || r.left < b.left - 1.5) issues.push({kind:'card', el: label(el), box: label(box).slice(0,60), over: Math.round(Math.max(r.right-b.right, b.left-r.left))});
      }
      const cs = getComputedStyle(el);
      if(el.children.length === 0 && (el.textContent||'').trim() && el.scrollWidth > el.clientWidth + 1
         && cs.overflowX !== 'visible' && cs.textOverflow !== 'ellipsis' && !el.closest(scrollers) && el.tagName !== 'TEXTAREA' && el.tagName !== 'INPUT')
        issues.push({kind:'clipped', el: label(el)});
    }
  }
  if(document.documentElement.scrollWidth > vw + 1) issues.push({kind:'page-hscroll', w: document.documentElement.scrollWidth, vw});
  const out = [];
  for(const i of issues){ const k = i.kind + i.el; if(!seen.has(k)){ seen.add(k); out.push(i); } }
  return out;
}
