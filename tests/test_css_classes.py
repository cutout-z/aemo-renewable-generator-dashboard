"""Every class index.html uses must exist in the compiled stylesheet it loads.

Incident this guards: a by-path carry published a new index.html without the rebuilt assets/css/app.css,
so Pages served new markup on the old stylesheet (see AGENTS.md pitfalls). Tailwind purges unseen
classes, so a class that is in the page but not in app.css renders unstyled.
"""

from page_harness import ROOT, defined_classes, inline_css, missing_classes, used_classes

HTML = (ROOT / "index.html").read_text()
CSS = (ROOT / "assets" / "css" / "app.css").read_text()

# Classes the page uses that app.css legitimately does not define. Each needs a reason; a stale
# entry (no longer used, or now defined) fails the second test.
ALLOWED_UNSTYLED = {}  # none today: every class the page uses is styled


def test_every_used_class_is_in_the_compiled_css():
    missing = missing_classes(HTML, CSS) - set(ALLOWED_UNSTYLED)
    assert not missing, (
        f"classes used in index.html but not defined in assets/css/app.css: {sorted(missing)}. "
        "Rebuild app.css and commit it with index.html, or allowlist a JS hook class with a reason."
    )


def test_allowlist_has_no_stale_entries():
    used, _ = used_classes(HTML)
    defined = defined_classes(CSS + "\n" + inline_css(HTML))
    stale = sorted(c for c in ALLOWED_UNSTYLED if c not in used or c in defined)
    assert not stale, f"allowlist entries no longer needed (unused or now styled): {stale}"


def test_the_stylesheet_is_the_one_the_page_loads():
    assert 'href="assets/css/app.css"' in HTML
    assert len(CSS) > 10_000, "assets/css/app.css looks empty or truncated"
