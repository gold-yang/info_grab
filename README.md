# 实时金融监测看板（网页模式）

按你的要求：

- 只做网页监测展示（`/`）
- 通过 SSE（`/stream`）实时更新页面
- 重要信息固定展示在页面顶部：**汇率（USD/CNY）/ BTC 实时价格 / 10Y利率**
- 即使网络波动也不关闭这些核心信息卡片：优先显示上次有效值，否则显示 `--`

## 数据来源（免费公开）

- Yahoo Finance（多资产行情、利率、美股、贵金属等）
- ER-API（FX）
- CoinGecko（BTC）
- Fed / BLS / CoinDesk / Google News RSS（事件与城市融资热度）

> 网络受限时会显示空数据或上次有效值，不会伪造。

## 启动

```bash
python3 app.py
```

浏览器打开：`http://127.0.0.1:8000`


## 兼容接口（用于旧前端，避免404）

虽然当前页面主流程仅依赖 `/stream`，但服务仍提供只读兼容接口：

- `GET /api/meta`
- `GET /api/snapshot`
- `GET /api/panel?tab=FX|Rates|Crypto|Equities|FixedIncome|Commodities`
- `GET /api/events`
- `GET /api/macro_calendar`
- `GET /api/city_financing`
