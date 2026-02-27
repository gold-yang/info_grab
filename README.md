# 实时金融监测看板（商业级保留策略）

按你的要求实现：

- 重要数据固定置顶实时刷新：`USD/CNY`、`BTC/USD`、`US 10Y`、`US 2Y`
- **无新数据时不清理旧数据**（全板块生效）
- 只有拿到新的实时值才替换对应旧值
- 页面通过 `/stream` 实时推送更新（5秒）

## 关键策略

- 状态存储 `StateStore`
- 行情合并 `merge_rows`：新值为空则保留旧值
- 事件合并 `merge_events`：去重增量更新
- 城市热度合并 `merge_cities`：抓取失败时保留旧城市记录

## 启动

```bash
python3 app.py
```

浏览器打开：`http://127.0.0.1:8000`

## 兼容接口（只读，避免旧前端404）

- `GET /api/meta`
- `GET /api/snapshot`
- `GET /api/panel?tab=FX|Rates|Crypto|Equities|FixedIncome|Commodities`
- `GET /api/events`
- `GET /api/macro_calendar`
- `GET /api/city_financing`
