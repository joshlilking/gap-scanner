# Stock Gap Screener

Scans the entire US stock market (Nasdaq, NYSE, AMEX) for gap-down setups on daily charts.

## Strategy
- **Detect**: Stocks that gap down significantly on daily chart
- **Entry**: Daily close above HIGH of the gap-down candle
- **Stop Loss**: Below LOW of the gap-down candle
- **Take Profit**: LOW of the candle before the gap (gap fill target)
- **Trailing Stop**: Configurable - every N% gain, raise SL by M%

## Features
- Scans ~6,600+ US stocks from Nasdaq's full traded database
- Supplementary tickers from Finviz and Barchart screeners
- Filters: gap %, price, market cap, volume, lookback days
- Live price refresh via yfinance
- Unfilled gap detection (skips already-filled gaps)
- Validates gap quality (rejects intraday-filled gaps and negative reward setups)
- WhatsApp alerts via CallMeBot
- CSV download with all candle data and settings
- Dark-themed dashboard with candlestick charts
- Sort by gap size, R/R, date, symbol

## Setup
```bash
pip install -r requirements.txt
python gap_scanner.py
```
Runs on `http://localhost:5558`

## Deployment
Designed to run on Windows Server behind Cloudflare Tunnel. The `bot_service.py` master launcher manages it as a service thread.

## API Endpoints
- `GET /api/gaps` - All setups (supports `?status=`, `?sort=`, `?order=`)
- `GET /api/gaps/scan` - Trigger scan (async, returns immediately)
- `GET /api/gaps/scan/status` - Poll scan progress
- `GET /api/gaps/settings` - Get current settings
- `POST /api/gaps/settings` - Update settings
- `GET /api/gaps/prices` - Refresh live prices
- `GET /api/gaps/download` - Download CSV
- `GET /api/gaps/chart/{symbol}` - Candlestick chart data
- `POST /api/gaps/{symbol}/activate` - Enter trade
- `POST /api/gaps/{symbol}/close` - Close trade
- `POST /api/gaps/clear` - Clear all watching setups
- `GET /api/gaps/active` - Active trades only
- `GET /api/gaps/stats` - Win rate, P&L stats
