from __future__ import annotations

import json
import threading
import time
import xml.etree.ElementTree as ET
from datetime import datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from queue import Empty, Queue
from typing import Dict, List, Tuple
from urllib.parse import parse_qs, quote_plus, urlparse
from urllib.request import Request, urlopen

HOST = "0.0.0.0"
PORT = 8000
PUSH_INTERVAL_SEC = 5

LOCK = threading.Lock()
SUBSCRIBERS: List[Queue] = []
CACHE: Dict[str, Tuple[float, object]] = {}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fetch(url: str, timeout: int = 8) -> bytes:
    req = Request(url, headers={"User-Agent": "info-grab/1.0"})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read()


def safe_json(url: str, default):
    try:
        return json.loads(fetch(url).decode("utf-8"))
    except Exception:
        return default


def cached(key: str, ttl_sec: int, loader):
    now = time.time()
    with LOCK:
        hit = CACHE.get(key)
        if hit and now - hit[0] < ttl_sec:
            return hit[1]
    val = loader()
    with LOCK:
        CACHE[key] = (now, val)
    return val


class StateStore:
    """商业级核心策略：只有拿到新数据时替换；没拿到就保留旧数据。"""

    def __init__(self):
        self.state = {
            "ts": now_str(),
            "note": "实时模式：无新数据时保留旧值；仅在获取到新值时替换。",
            "important": [],
            "snapshot": [],
            "board": {"汇率": [], "利率": [], "BTC": [], "美股": [], "美债": [], "贵金属": []},
            "events": [],
            "cities": [],
        }

    @staticmethod
    def _merge_rows(old_rows: List[Dict], new_rows: List[Dict], key: str = "symbol") -> List[Dict]:
        if not new_rows:
            return old_rows
        merged = {r.get(key): dict(r) for r in old_rows if r.get(key)}
        for r in new_rows:
            k = r.get(key)
            if not k:
                continue
            if r.get("value") is None and k in merged:
                # 无新值则保留旧值
                continue
            merged[k] = dict(r)
        # 新数据优先排序
        ordered = [merged[r.get(key)] for r in new_rows if r.get(key) in merged]
        seen = {r.get(key) for r in ordered}
        ordered += [v for k, v in merged.items() if k not in seen]
        return ordered

    @staticmethod
    def _merge_events(old_rows: List[Dict], new_rows: List[Dict]) -> List[Dict]:
        if not new_rows:
            return old_rows
        keys = {f"{x.get('title')}|{x.get('link')}" for x in old_rows}
        out = list(old_rows)
        for r in new_rows:
            k = f"{r.get('title')}|{r.get('link')}"
            if k not in keys:
                out.insert(0, r)
                keys.add(k)
        return out[:30]

    @staticmethod
    def _merge_cities(old_rows: List[Dict], new_rows: List[Dict]) -> List[Dict]:
        if not new_rows:
            return old_rows
        old_map = {x.get("city"): x for x in old_rows}
        out = []
        for r in new_rows:
            city = r.get("city")
            if not city:
                continue
            if r.get("count", 0) == 0 and city in old_map:
                out.append(old_map[city])
            else:
                out.append(r)
        # 把旧城市补齐，避免因抓取失败消失
        existing = {x.get("city") for x in out}
        out += [v for k, v in old_map.items() if k not in existing]
        out.sort(key=lambda x: x.get("count", 0), reverse=True)
        return out[:20]

    def merge(self, new_payload: Dict) -> None:
        self.state["ts"] = now_str()
        self.state["note"] = new_payload.get("note", self.state["note"])

        self.state["important"] = self._merge_rows(self.state["important"], new_payload.get("important", []), key="symbol")
        self.state["snapshot"] = self._merge_rows(self.state["snapshot"], new_payload.get("snapshot", []), key="symbol")

        old_board = self.state.get("board", {})
        new_board = new_payload.get("board", {})
        merged_board = {}
        for cat in ["汇率", "利率", "BTC", "美股", "美债", "贵金属"]:
            merged_board[cat] = self._merge_rows(old_board.get(cat, []), new_board.get(cat, []), key="symbol")
        self.state["board"] = merged_board

        self.state["events"] = self._merge_events(self.state["events"], new_payload.get("events", []))
        self.state["cities"] = self._merge_cities(self.state["cities"], new_payload.get("cities", []))

    def snapshot(self) -> Dict:
        return json.loads(json.dumps(self.state, ensure_ascii=False))


STORE = StateStore()


def yahoo(symbols: List[str]) -> List[Dict]:
    def load():
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={quote_plus(','.join(symbols))}"
        data = safe_json(url, {"quoteResponse": {"result": []}})
        rows = []
        for r in data.get("quoteResponse", {}).get("result", []):
            rows.append(
                {
                    "name": r.get("shortName") or r.get("longName") or r.get("symbol"),
                    "symbol": r.get("symbol"),
                    "value": r.get("regularMarketPrice"),
                    "change_pct": r.get("regularMarketChangePercent"),
                    "source": "Yahoo Finance",
                    "stale": False,
                }
            )
        return rows

    return cached("yahoo_" + "_".join(symbols), 20, load)


def fx_block() -> List[Dict]:
    data = safe_json("https://open.er-api.com/v6/latest/USD", {})
    rates = data.get("rates", {}) if isinstance(data, dict) else {}
    rows = []
    for c in ["CNY", "JPY", "EUR", "RUB"]:
        rows.append({"name": f"USD/{c}", "symbol": f"USD/{c}", "value": rates.get(c), "change_pct": None, "source": "ER-API", "stale": False})
    return rows


def btc_block() -> List[Dict]:
    data = safe_json("https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true", {})
    b = data.get("bitcoin", {}) if isinstance(data, dict) else {}
    return [{"name": "Bitcoin", "symbol": "BTCUSD", "value": b.get("usd"), "change_pct": b.get("usd_24h_change"), "source": "CoinGecko", "stale": False}]


def parse_rss(url: str, source: str, tag: str, limit: int = 6) -> List[Dict]:
    try:
        root = ET.fromstring(fetch(url, timeout=12))
    except Exception:
        return []
    rows = []
    for item in root.findall(".//item")[:limit]:
        title = (item.findtext("title") or "").strip()
        link = (item.findtext("link") or "").strip()
        pub = (item.findtext("pubDate") or now_str()).strip()
        if title and link:
            rows.append({"title": title, "link": link, "time": pub, "source": source, "tag": tag})
    return rows


def collect_live_data() -> Dict:
    fx = fx_block()
    btc = btc_block()
    rates = yahoo(["^IRX", "^FVX", "^TNX", "^TYX"])

    usd_cny = next((x for x in fx if x.get("symbol") == "USD/CNY"), None)
    btc_usd = btc[0] if btc else None
    us10y = next((x for x in rates if x.get("symbol") == "^TNX"), None)
    us2y = next((x for x in rates if x.get("symbol") == "^FVX"), None)

    important = [x for x in [
        {"label": "USD/CNY", "symbol": "USD/CNY", "value": (usd_cny or {}).get("value"), "source": "ER-API", "stale": False},
        {"label": "BTC/USD", "symbol": "BTCUSD", "value": (btc_usd or {}).get("value"), "source": "CoinGecko", "stale": False},
        {"label": "US 10Y Yield", "symbol": "^TNX", "value": (us10y or {}).get("value"), "source": "Yahoo Finance", "stale": False},
        {"label": "US 2Y Yield", "symbol": "^FVX", "value": (us2y or {}).get("value"), "source": "Yahoo Finance", "stale": False},
    ]]

    board = {
        "汇率": fx + yahoo(["EURUSD=X", "USDRUB=X"]),
        "利率": rates,
        "BTC": btc,
        "美股": yahoo(["^GSPC", "^IXIC", "^DJI"]),
        "美债": yahoo(["^FVX", "^TNX", "^TYX"]),
        "贵金属": yahoo(["GC=F", "SI=F"]),
    }

    snapshot = yahoo(["^GSPC", "^IXIC", "GC=F", "SI=F", "CL=F", "^TNX", "USDCNY=X", "USDJPY=X"]) 

    events = []
    events += parse_rss("https://www.federalreserve.gov/feeds/press_monetary.xml", "Fed", "央行", 4)
    events += parse_rss("https://www.bls.gov/feed/bls_latest.rss", "BLS", "宏观", 4)
    events += parse_rss("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk", "Crypto", 4)
    events = events[:12]

    cities = []
    for c in ["北京", "上海", "深圳", "广州", "杭州", "南京", "苏州", "成都", "重庆", "武汉"]:
        rss = parse_rss(f"https://news.google.com/rss/search?q={quote_plus(c + ' 融资 投资')}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans", "GoogleNews", "城市融资", 8)
        cities.append({"city": c, "count": len(rss), "top": rss[0]["title"] if rss else "暂无"})
    cities.sort(key=lambda x: x["count"], reverse=True)

    return {
        "note": "商业级策略：无新数据不清空旧数据，只有新实时值才替换。",
        "important": important,
        "snapshot": snapshot,
        "board": board,
        "events": events,
        "cities": cities,
    }


HTML_PAGE = """<!doctype html><html lang='zh-CN'><head><meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/><title>实时金融监测看板</title>
<style>
body{margin:0;font-family:Inter,Arial,"PingFang SC";background:#0b1220;color:#e7eeff}.wrap{max-width:1300px;margin:0 auto;padding:16px}
.card{background:#121d34;border:1px solid #ffffff22;border-radius:10px;padding:10px;margin-bottom:10px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}.grid4{display:grid;grid-template-columns:repeat(4,1fr);gap:10px}
.table{width:100%;font-size:12px;border-collapse:collapse}.table th,.table td{padding:4px;border-bottom:1px solid #ffffff18;text-align:left}
.item{padding:6px 0;border-bottom:1px solid #ffffff18}.item a{color:#cde1ff;text-decoration:none}.muted{color:#9cb0da;font-size:12px}
.big{font-size:24px;font-weight:700}.stale{color:#ffbe70}
@media(max-width:1100px){.grid3{grid-template-columns:1fr}.grid4{grid-template-columns:1fr 1fr}.grid2{grid-template-columns:1fr}}
</style></head><body><div class='wrap'>
<div class='card'><h2 style='margin:0'>实时金融监测看板</h2><div class='muted' id='meta'></div></div>
<div class='grid4' id='important' style='position:sticky;top:8px;z-index:5'></div>
<div class='card'><h3>市场快照</h3><table class='table' id='snapshot'></table></div>
<div class='grid3' id='board'></div>
<div class='grid2'>
  <div class='card'><h3>事件流</h3><div id='events'></div></div>
  <div class='card'><h3>城市融资热度</h3><table class='table' id='cities'></table></div>
</div>
</div>
<script>
function fmt(v){return (v===null||v===undefined)?'--':v}

function ensureTable(el, headers){
  if(!el.tHead){
    const thead=el.createTHead();
    const tr=thead.insertRow();
    headers.forEach(h=>{const th=document.createElement('th');th.textContent=h;tr.appendChild(th);});
  }
  if(!el.tBodies.length){el.appendChild(document.createElement('tbody'));}
  return el.tBodies[0];
}

function patchRows(tableEl, headers, rows, keyIndex=1){
  const tbody=ensureTable(tableEl, headers);
  const existing=new Map([...tbody.querySelectorAll('tr')].map(tr=>[tr.dataset.key,tr]));
  const used=new Set();
  for(const row of rows){
    const key=String(row[keyIndex] ?? row[0] ?? Math.random());
    let tr=existing.get(key);
    if(!tr){tr=document.createElement('tr'); tr.dataset.key=key; tbody.appendChild(tr);} 
    used.add(key);
    const cells=row.map(v=>String(v ?? ''));
    while(tr.children.length<cells.length){tr.appendChild(document.createElement('td'));}
    while(tr.children.length>cells.length){tr.removeChild(tr.lastChild);}    
    cells.forEach((c,i)=>{if(tr.children[i].textContent!==c) tr.children[i].textContent=c;});
  }
  for(const [k,tr] of existing){ if(!used.has(k)) tr.remove(); }
}

function patchImportant(items){
  const root=document.getElementById('important');
  const existing=new Map([...root.querySelectorAll('[data-key]')].map(n=>[n.dataset.key,n]));
  const used=new Set();
  for(const x of (items||[])){
    const key=x.symbol||x.label;
    let card=existing.get(key);
    if(!card){
      card=document.createElement('div'); card.className='card'; card.dataset.key=key;
      card.innerHTML="<div class='muted k'></div><div class='big v'></div><div class='muted s'></div>";
      root.appendChild(card);
    }
    used.add(key);
    card.querySelector('.k').textContent=x.label||key;
    card.querySelector('.v').textContent=fmt(x.value);
    const s=card.querySelector('.s');
    s.textContent=(x.source||'') + (x.stale?' · 使用旧值':'');
    s.className='muted s' + (x.stale?' stale':'');
  }
  for(const [k,n] of existing){ if(!used.has(k)) n.remove(); }
}

function patchBoard(board){
  const root=document.getElementById('board');
  const existing=new Map([...root.querySelectorAll('[data-cat]')].map(n=>[n.dataset.cat,n]));
  const cats=Object.keys(board||{});
  for(const c of cats){
    let card=existing.get(c);
    if(!card){
      card=document.createElement('div'); card.className='card'; card.dataset.cat=c;
      card.innerHTML=`<h3>${c}</h3><table class='table'></table>`;
      root.appendChild(card);
    }
    const rows=(board[c]||[]).map(x=>[x.name,x.symbol,fmt(x.value),fmt(x.change_pct),x.source]);
    patchRows(card.querySelector('table'), ['name','symbol','value','chg%','source'], rows, 1);
  }
}

function patchEvents(events){
  const root=document.getElementById('events');
  if(!events || !events.length){if(!root.children.length) root.innerHTML='<div class="muted">暂无</div>'; return;}
  root.innerHTML='';
  for(const e of events){
    const div=document.createElement('div'); div.className='item';
    div.innerHTML=`<a target='_blank' href='${e.link}'>${e.title}</a><div class='muted'>${e.time} · ${e.source} · ${e.tag}</div>`;
    root.appendChild(div);
  }
}

function render(payload){
  document.getElementById('meta').textContent=`更新时间: ${payload.ts} | ${payload.note}`;
  patchImportant(payload.important||[]);
  patchRows(document.getElementById('snapshot'), ['name','symbol','value','chg%','source'], (payload.snapshot||[]).map(x=>[x.name,x.symbol,fmt(x.value),fmt(x.change_pct),x.source]), 1);
  patchBoard(payload.board||{});
  patchEvents(payload.events||[]);
  patchRows(document.getElementById('cities'), ['city','news_count','top_news'], (payload.cities||[]).map(x=>[x.city,x.count,x.top]), 0);
}
let es=null;
function connect(){es=new EventSource('/stream');es.onmessage=(e)=>{try{render(JSON.parse(e.data))}catch(_){}};es.onerror=()=>{try{es.close()}catch(_){};setTimeout(connect,3000);};}
connect();
</script>"""


class Handler(BaseHTTPRequestHandler):
    def _send_json(self, data):
        b = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(b)))
        self.end_headers()
        self.wfile.write(b)

    def do_GET(self):
        p = urlparse(self.path)
        if p.path == "/":
            body = HTML_PAGE.encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        # compatibility readonly routes
        snap = STORE.snapshot()
        if p.path == "/api/meta":
            self._send_json({"ts": snap["ts"], "note": snap["note"]})
            return
        if p.path == "/api/snapshot":
            self._send_json(snap["snapshot"])
            return
        if p.path == "/api/panel":
            tab = (parse_qs(p.query).get("tab", ["FX"])[0] or "FX").strip()
            mapping = {"FX": "汇率", "Rates": "利率", "Crypto": "BTC", "Equities": "美股", "FixedIncome": "美债", "Commodities": "贵金属"}
            self._send_json(snap["board"].get(mapping.get(tab, "汇率"), []))
            return
        if p.path == "/api/events":
            self._send_json(snap["events"])
            return
        if p.path == "/api/macro_calendar":
            self._send_json([])
            return
        if p.path == "/api/city_financing":
            self._send_json([{"city_name": x["city"], "equity_deal_count": x["count"], "key_events": [{"title": x["top"]}]} for x in snap["cities"]])
            return

        if p.path == "/stream":
            self.send_response(HTTPStatus.OK)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            q: Queue = Queue()
            with LOCK:
                SUBSCRIBERS.append(q)
            try:
                first = STORE.snapshot()
                self.wfile.write(f"data: {json.dumps(first, ensure_ascii=False)}\n\n".encode("utf-8"))
                self.wfile.flush()
                while True:
                    try:
                        payload = q.get(timeout=20)
                    except Empty:
                        payload = {"ping": now_str()}
                    self.wfile.write(f"data: {json.dumps(payload, ensure_ascii=False)}\n\n".encode("utf-8"))
                    self.wfile.flush()
            except Exception:
                pass
            finally:
                with LOCK:
                    if q in SUBSCRIBERS:
                        SUBSCRIBERS.remove(q)
            return

        self.send_error(404)


def broadcaster() -> None:
    while True:
        try:
            live = collect_live_data()
            STORE.merge(live)
            payload = STORE.snapshot()
            with LOCK:
                for q in list(SUBSCRIBERS):
                    try:
                        q.put_nowait(payload)
                    except Exception:
                        SUBSCRIBERS.remove(q)
        except Exception:
            # never crash background loop
            pass
        time.sleep(PUSH_INTERVAL_SEC)


if __name__ == "__main__":
    # non-blocking startup: do not wait for external data before serving UI
    threading.Thread(target=broadcaster, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Server running on http://127.0.0.1:{PORT}")
    server.serve_forever()
