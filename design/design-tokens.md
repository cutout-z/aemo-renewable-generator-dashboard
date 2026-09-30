# Design tokens — the rules

The values live in **`assets/css/tailwind.src.css`** (one `:root` block, dark + light) and compile to
`assets/css/app.css`, which is **committed** — GitHub Pages serves the repo, so nothing builds at
deploy time. This file states the rules the CSS cannot.

This page has no chart library: heat is HTML cells, stepped by the `.seq-*` classes that mirror the
same ramp the Plotly dashboards use (`ChartTokens.seq()`). One value → one colour, on every page.

## The rules

1. **No raw hex in `index.html` or `design/*.html`.** Use the token names (`--accent`, `.card`, `.th`,
   `.seq-3`). The token source is the only file with literal colours; `scripts/verify-design.py`
   fails the pass on a hex colour in the page.
2. **Colour belongs to an entity, not to a row or a column index.** The family a metric belongs to —
   actual curtailment, projected (ELI), MLF, ISP curtailment, ISP offloading — keeps its colour
   wherever it appears, and heat *intensity* comes from the single ramp. Nothing is coloured
   "because it is the third series".
3. **Colour is never the only signal.** Every heat cell prints its number; a group header carries
   the words; a value that does not exist is stated (`N/A`, "not published for this window"), never
   left as a blank cell or a zero that reads as data.
4. **`--faint` is the floor for 12px text** (4.9:1 on a card in dark, 4.7:1 on white in light).
   Nothing dimmer carries text.
5. **Two themes, one code path.** Dark is the default; `<html data-theme="light">` flips it (and
   `?theme=light` / `?theme=dark` force one for screenshots and shared links). A hard-coded
   dark-only colour breaks the flip — the heat ramp has a light variant too (`--seq-*`, `--seq-t*`).
6. **Heat is the sequential ramp (`.seq-0 … .seq-7`) with a stated scale.** The legend names the
   numeric stops and the direction. A heatmap whose scale is not on screen is a guess, not a reading.
7. **`.card` is the unit of composition**: `.card-head` owns the title and the one-line explanation,
   `.card-body` the content, `.card-foot` the provenance — source, publication lag, as-of date.
   Provenance is not decoration here: these pages publish monthly, so *as-of* is part of the answer.

## Adding classes

Tailwind only emits component classes it finds in the sources (`content` in `tailwind.config.js`:
`index.html` and `design/**/*.html`). After adding any class name:

```bash
./scripts/build-css.sh      # rewrites assets/css/app.css — commit it
```

A class that exists only in a string the scanner cannot see silently does nothing — that failure
looks exactly like "the styling didn't apply".
