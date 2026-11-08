# Perp Spread Desk

Live cross-venue analytics for perpetual futures on **Hyperliquid** (every dex), **Lighter** and **Variational**:
prices side by side, funding spreads, divergence history with every episode timestamped, and an event feed
that logs when a gap opens or closes.

Live site: https://0xsatoshinakamoto.github.io/perp-spread-desk/

## Arb scanner

Six rule-based agents run in the browser and grade every same-asset pair:
pair finder → history (candles from Hyperliquid and Lighter) → session tagger (US hours, overnight, weekend)
→ quality guard (one-venue spikes, stale books, standing premiums, crypto / pre-IPO risk, thin volume)
→ backtester (your entry/exit gap rule, fills one bar late, fees + slippage) → judge (Tradeable / Watch / Avoid).
Results come from last-trade candle prices; confirm the live bid/ask gap before trading. Funding is not included.

- Hyperliquid and Lighter stream over WebSocket (about one update per second); Variational is polled every 5 s.
- Everything runs in the browser: one `index.html`, no build step, no server, no API keys.
- Code lives in `app.js`. To add an exchange, copy the `TEMPLATE` adapter near the top of `app.js`, add it to `VENUES`,
  and add the exchange's `https://` / `wss://` address to `connect-src` in the Content-Security-Policy line of `index.html`.

## Security

- No accounts, wallets, keys or user data: the page only reads public market data.
- Content-Security-Policy allows only this site's own `app.js`, the pinned chart library, and the listed exchange APIs.
- The chart library is pinned to an exact version with a Subresource Integrity hash.
- Every value received from an exchange is escaped before it is shown.

Variational quotes can be 20–80 s old; quotes older than the "Max quote age" setting are left out of gap numbers.
Pre-IPO markets price off each venue's own order book, so their gaps are not guaranteed to close.
