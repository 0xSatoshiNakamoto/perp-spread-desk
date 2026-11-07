# Perp Spread Desk

Live cross-venue analytics for perpetual futures on **Hyperliquid** (every dex), **Lighter** and **Variational**:
prices side by side, funding spreads, divergence history with every episode timestamped, and an event feed
that logs when a gap opens or closes.

Live site: https://0xsatoshinakamoto.github.io/perp-spread-desk/

- Hyperliquid and Lighter stream over WebSocket (about one update per second); Variational is polled every 5 s.
- Everything runs in the browser: one `index.html`, no build step, no server, no API keys.
- To add an exchange, copy the `TEMPLATE` adapter near the top of the script in `index.html` and add it to `VENUES`.

Variational quotes can be 20–80 s old; quotes older than the "Max quote age" setting are left out of gap numbers.
Pre-IPO markets price off each venue's own order book, so their gaps are not guaranteed to close.
