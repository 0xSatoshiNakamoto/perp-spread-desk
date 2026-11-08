# Perp Spread Desk

Live cross-venue analytics for perpetual futures on **Hyperliquid** (every dex), **Lighter** and **Variational**:
prices side by side, funding spreads, divergence history with every episode timestamped, and an event feed
that logs when a gap opens or closes.

Live site: https://0xsatoshinakamoto.github.io/perp-spread-desk/

## Arb scanner

Eight rule-based agents run in the browser and follow a gap-trading rule:
1. **Enter** when the *fillable* gap reaches the entry level (default 10 bps): selling your size into the rich
   venue's bids and buying it from the cheap venue's asks. Gaps between mid prices that are only the spreads are
   thrown out.
2. Count it only if the gap **lasts** long enough to act on (default 1 hour), acting after a reaction delay (15 min).
3. **Close** when the gap shrinks to the exit level (3 bps) or flips, by the deadline (same day by default);
   optionally ride a flip with a trailing give-back.
4. **Costs** come from the live order books: market orders pay taker fees and walk the book for your size,
   limit orders pay maker fees (and may not fill on a thin book).

Agents: pair finder → history → session tagger → opportunity spotter → quality guard (one-venue spikes,
stale books, crypto / pre-IPO risk) → order-book checker → trade simulator → analyst (verdicts and a
daily summary of the last three days). A live panel prices gaps open right now off both books.
Live views, alerts and the "Right now" panel all rank by the fill gap (sell at the bid, buy at the ask), not mid prices.
Results use last-trade candle prices plus today's books; check the live books before trading. Funding is not included.

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
