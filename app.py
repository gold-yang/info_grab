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

LOCK = threading.Lock()
SUBSCRIBERS: List[Queue] = []
CACHE: Dict[str, Tuple[float, object]] = {}
LAST_VALUES: Dict[str, Dict] = {}


def now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def fetch(url: str, timeout: int = 12) -> bytes:
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
    value = loader()
    with LOCK:
        CACHE[key] = (now, value)
    return value


def remember(symbol: str, value, source: str) -> Dict:
    with LOCK:
        if value is not None:
            LAST_VALUES[symbol] = {"value": value, "ts": now_str(), "source": source, "stale": False}
            return LAST_VALUES[symbol]
        old = LAST_VALUES.get(symbol)
        if old:
            return {**old, "stale": True}
    return {"value": None, "ts": now_str(), "source": source, "stale": True}


def yahoo(symbols: List[str]) -> List[Dict]:
    def load():
        url = f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={quote_plus(','.join(symbols))}"
        data = safe_json(url, {"quoteResponse": {"result": []}})
        out = []
        for r in data.get("quoteResponse", {}).get("result", []):
            out.append(
                {
                    "name": r.get("shortName") or r.get("longName") or r.get("symbol"),
                    "symbol": r.get("symbol"),
                    "value": r.get("regularMarketPrice"),
                    "change_pct": r.get("regularMarketChangePercent"),
                    "source": "Yahoo Finance",
                }
            )
        return out

    return cached("yahoo_" + "_".join(symbols), 20, load)


def fx_block() -> List[Dict]:
    data = safe_json("https://open.er-api.com/v6/latest/USD", {})
    rates = data.get("rates", {}) if isinstance(data, dict) else {}
    rows = []
    for c in ["CNY", "JPY", "EUR", "RUB"]:
        rows.append(
            {
                "name": f"USD/{c}",
                "symbol": f"USD/{c}",
                "value": rates.get(c),
                "change_pct": None,
                "source": "ER-API",
            }
        )
    return rows


def btc_block() -> List[Dict]:
    data = safe_json(
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin&vs_currencies=usd&include_24hr_change=true",
        {},
    )
    b = data.get("bitcoin", {}) if isinstance(data, dict) else {}
    return [
        {
            "name": "Bitcoin",
            "symbol": "BTCUSD",
            "value": b.get("usd"),
            "change_pct": b.get("usd_24h_change"),
            "source": "CoinGecko",
        }
    ]


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


def collect_data() -> Dict:
    snapshot = yahoo(["^GSPC", "^IXIC", "GC=F", "SI=F", "CL=F", "^TNX", "USDCNY=X", "USDJPY=X"])

    fx = fx_block()
    btc = btc_block()
    rates = yahoo(["^IRX", "^TNX", "^TYX"])

    # 重要信息固定显示（即使源暂时失败，也显示上次值）
    usd_cny = next((x for x in fx if x["symbol"] == "USD/CNY"), {})
    btc_usd = btc[0] if btc else {}
    us10y = next((x for x in rates if x.get("symbol") == "^TNX"), {})

    important = [
        {
            "label": "USD/CNY",
            "symbol": "USD/CNY",
            "value": remember("USD/CNY", usd_cny.get("value"), "ER-API")["value"],
            "source": "ER-API",
            "stale": remember("USD/CNY", usd_cny.get("value"), "ER-API")["stale"],
        },
        {
            "label": "BTC/USD",
            "symbol": "BTCUSD",
            "value": remember("BTCUSD", btc_usd.get("value"), "CoinGecko")["value"],
            "source": "CoinGecko",
            "stale": remember("BTCUSD", btc_usd.get("value"), "CoinGecko")["stale"],
        },
        {
            "label": "US 10Y Yield",
            "symbol": "^TNX",
            "value": remember("^TNX", us10y.get("value"), "Yahoo Finance")["value"],
            "source": "Yahoo Finance",
            "stale": remember("^TNX", us10y.get("value"), "Yahoo Finance")["stale"],
        },
    ]

    board = {
        "汇率": fx + [x for x in yahoo(["EURUSD=X", "USDRUB=X"])],
        "利率": rates,
        "BTC": btc,
        "美股": yahoo(["^GSPC", "^IXIC", "^DJI"]),
        "美债": yahoo(["^FVX", "^TNX", "^TYX"]),
        "贵金属": yahoo(["GC=F", "SI=F"]),
    }

    events = []
    events += parse_rss("https://www.federalreserve.gov/feeds/press_monetary.xml", "Fed", "央行", 4)
    events += parse_rss("https://www.bls.gov/feed/bls_latest.rss", "BLS", "宏观", 4)
    events += parse_rss("https://www.coindesk.com/arc/outboundfeeds/rss/", "CoinDesk", "Crypto", 4)
    events = events[:12]

    cities = []
    for c in ["北京", "上海", "深圳", "广州", "杭州", "南京", "苏州", "成都", "重庆", "武汉"]:
        rss = parse_rss(
            f"https://news.google.com/rss/search?q={quote_plus(c + ' 融资 投资')}&hl=zh-CN&gl=CN&ceid=CN:zh-Hans",
            "GoogleNews",
            "城市融资",
            8,
        )
        cities.append({"city": c, "count": len(rss), "top": rss[0]["title"] if rss else "暂无"})
    cities.sort(key=lambda x: x["count"], reverse=True)

    return {
        "ts": now_str(),
        "important": important,
        "snapshot": snapshot,
        "board": board,
        "events": events,
        "cities": cities,
        "note": "重要信息固定显示；网络受限时显示上次值或 --，不会虚构。",
    }


HTML_PAGE = """<!doctype html><html lang='zh-CN'><head><meta charset='UTF-8'/><meta name='viewport' content='width=device-width,initial-scale=1'/><title>实时金融监测看板</title>
<style>
body{margin:0;font-family:Inter,Arial,"PingFang SC";background:#0b1220;color:#e7eeff}.wrap{max-width:1300px;margin:0 auto;padding:16px}
.card{background:#121d34;border:1px solid #ffffff22;border-radius:10px;padding:10px;margin-bottom:10px}
.grid2{display:grid;grid-template-columns:1fr 1fr;gap:10px}.grid3{display:grid;grid-template-columns:repeat(3,1fr);gap:10px}
.table{width:100%;font-size:12px;border-collapse:collapse}.table th,.table td{padding:4px;border-bottom:1px solid #ffffff18;text-align:left}
.item{padding:6px 0;border-bottom:1px solid #ffffff18}.item a{color:#cde1ff;text-decoration:none}.muted{color:#9cb0da;font-size:12px}
.big{font-size:24px;font-weight:700}.stale{color:#ffbe70}
@media(max-width:1100px){.grid3{grid-template-columns:1fr}.grid2{grid-template-columns:1fr}}
</style></head><body><div class='wrap'>
<div class='card'><h2 style='margin:0'>实时金融监测看板</h2><div class='muted' id='meta'></div></div>
<div class='grid3' id='important'></div>
<div class='card'><h3>市场快照</h3><table class='table' id='snapshot'></table></div>
<div class='grid3' id='board'></div>
<div class='grid2'>
  <div class='card'><h3>事件流</h3><div id='events'></div></div>
  <div class='card'><h3>城市融资热度</h3><table class='table' id='cities'></table></div>
</div>
</div>
<script>
function t(headers, rows){return '<tr>'+headers.map(h=>`<th>${h}</th>`).join('')+'</tr>'+rows.map(r=>'<tr>'+r.map(c=>`<td>${c??""}</td>`).join('')+'</tr>').join('')}
function fmt(v){return (v===null||v===undefined)?'--':v}
function render(payload){
  document.getElementById('meta').textContent = `更新时间: ${payload.ts} | ${payload.note}`;

  document.getElementById('important').innerHTML = (payload.important||[]).map(x=>`<div class='card'><div class='muted'>${x.label}</div><div class='big'>${fmt(x.value)}</div><div class='muted ${x.stale?'stale':''}'>${x.source}${x.stale?' · 使用上次值':''}</div></div>`).join('');

  document.getElementById('snapshot').innerHTML = t(['name','symbol','value','chg%','source'], (payload.snapshot||[]).map(x=>[x.name,x.symbol,fmt(x.value),fmt(x.change_pct),x.source]));
  const b = payload.board || {};
  const cats = Object.keys(b);
  document.getElementById('board').innerHTML = cats.map(c=>{
    const rows=(b[c]||[]).map(x=>[x.name,x.symbol,fmt(x.value),fmt(x.change_pct),x.source]);
    return `<div class='card'><h3>${c}</h3><table class='table'>${t(['name','symbol','value','chg%','source'], rows)}</table></div>`;
  }).join('');
  document.getElementById('events').innerHTML = (payload.events||[]).map(e=>`<div class='item'><a target='_blank' href='${e.link}'>${e.title}</a><div class='muted'>${e.time} · ${e.source} · ${e.tag}</div></div>`).join('') || '<div class="muted">暂无</div>';
  document.getElementById('cities').innerHTML = t(['city','news_count','top_news'], (payload.cities||[]).map(x=>[x.city,x.count,x.top]));
}
const es = new EventSource('/stream');
es.onmessage = (e)=>{try{render(JSON.parse(e.data))}catch(_){}};
</script></body></html>"""


class Handler(BaseHTTPRequestHandler):
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

        # 兼容旧版前端请求，避免 /api/* 404 噪音
        if p.path == "/api/meta":
            payload = collect_data()
            body = json.dumps({"ts": payload["ts"], "note": payload["note"]}, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if p.path == "/api/snapshot":
            payload = collect_data()
            body = json.dumps(payload["snapshot"], ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if p.path == "/api/panel":
            tab = (parse_qs(p.query).get("tab", ["FX"])[0] or "FX").strip()
            payload = collect_data()
            mapping = {"FX": "汇率", "Rates": "利率", "Crypto": "BTC", "Equities": "美股", "FixedIncome": "美债", "Commodities": "贵金属"}
            rows = payload["board"].get(mapping.get(tab, "汇率"), [])
            body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if p.path == "/api/events":
            payload = collect_data()
            body = json.dumps(payload["events"], ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if p.path == "/api/macro_calendar":
            body = json.dumps([], ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
            return

        if p.path == "/api/city_financing":
            payload = collect_data()
            rows = [{"city_name": x["city"], "equity_deal_count": x["count"], "key_events": [{"title": x["top"]}]} for x in payload["cities"]]
            body = json.dumps(rows, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)
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
                first = collect_data()
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
        payload = collect_data()
        with LOCK:
            for q in list(SUBSCRIBERS):
                try:
                    q.put_nowait(payload)
                except Exception:
                    SUBSCRIBERS.remove(q)
        time.sleep(15)


if __name__ == "__main__":
    threading.Thread(target=broadcaster, daemon=True).start()
    server = ThreadingHTTPServer((HOST, PORT), Handler)
    print(f"Server running on http://127.0.0.1:{PORT}")
    server.serve_forever()
