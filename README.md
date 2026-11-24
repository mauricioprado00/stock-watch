# stock-watch

Live stock prices on a chart page, with spoken alarms. A small Python tool polls
Yahoo Finance for a list of companies and writes files that a single static page
(`index.html`) reads.

Published copy (a snapshot of the data committed here, not live):
https://mauricioprado00.github.io/stock-watch/

## Use

```sh
ln -s "$PWD/stock.py" ~/bin/stock
stock add COST NVDA          # add companies (loads ~4 weeks of history) and track them
stock start                  # track all; --every 10 for a check every 10 s
stock status                 # what is tracked, last price, last update
stock alarm COST '>=1095:sell'
stock stop
stock open COST              # the page in the browser
```

`stock --help` lists every command. On the page, `?` shows the keys: switch
stocks and ranges, page back through history, set alarms and markers, drag to
measure.

## How it works

- One tracker process polls every tracked company in a single request (up to
  20 per request) and keeps under a requests-per-hour budget.
- When tracking starts, the gap since the last stored price is filled with the
  finest bars Yahoo still serves for that age (1-minute up to 29 days back,
  then 5-minute, hourly, daily).
- Prices are stored per New York trading day in `store/SYMBOL/DAY.jsonl`.
- The page loads its data as `<script>` files, so it works straight from disk
  (`file://`). While the tracker runs it also serves a small API on 127.0.0.1
  for setting alarms and markers from the page.
- Alarms are spoken with `~/.claude/bin/claude-done` (Piper voice).

## Deploy

Pushing to `main` runs `.github/workflows/deploy-site.yml`, which moves the
`src/stock-watch` submodule of the private repo `githubio-private-pages` to the
new commit; that repo's push publishes mauricioprado00.github.io. It needs the
`SITE_DEPLOY_TOKEN` secret (see the workflow); without it the workflow skips.
