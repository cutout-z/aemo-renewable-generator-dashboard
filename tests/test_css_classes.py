"""Every class index.html uses must exist in the compiled stylesheet it loads.

Incident this guards: a by-path carry published a new index.html without the rebuilt assets/css/app.css,
so Pages served new markup on the old stylesheet (see AGENTS.md pitfalls). Tailwind purges unseen
classes, so a class that is in the page but not in app.css renders unstyled.
"""

import re

from page_harness import ROOT, defined_classes, inline_css, missing_classes, used_classes

HTML = (ROOT / "index.html").read_text()
CSS = (ROOT / "assets" / "css" / "app.css").read_text()

# Classes the page uses that app.css legitimately does not define. Each needs a reason; a stale
# entry (no longer used, or now defined) fails the second test.
ALLOWED_UNSTYLED = {}  # none today: every class the page uses is styled


def _list(name):
    """Items of the page's `const NAME = [...]` array literal."""
    m = re.search(rf"const {name} = \[([^\]]*)\]", HTML)
    assert m, f"index.html no longer defines {name}: update DYNAMIC below"
    return [x for x in m.group(1).split(",") if x.strip()]


# Dynamic class families: each prefix the page glues to a ${...} -> the concrete classes the JS can build.
# Derived from the page's own arrays, so a new heat step or group that the CSS lacks fails the test.
DYNAMIC = {
    "seq-": [f"seq-{i}" for i in range(max(len(_list("LOSS_LABELS")), len(_list("MLF_LABELS"))))],
    # `fam-${g}`: one column-group family per group key the page defines (the "meta" group has no header)
    "fam-": sorted(
        "fam-" + g
        for g in set(re.findall(r"group:\s*'([^']+)'", HTML)) | set(re.findall(r"GROUP_LABELS\['([^']+)'\]", HTML))
        if g != "meta"
    ),
}


def test_every_used_class_is_in_the_compiled_css():
    missing = missing_classes(HTML, CSS, DYNAMIC) - set(ALLOWED_UNSTYLED)
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


def test_every_dynamic_prefix_is_enumerated_and_nonempty():
    """A new `foo-${x}` family must be listed in DYNAMIC; an entry the page no longer builds is stale."""
    _, prefixes = used_classes(HTML)
    families = {p for p in prefixes if len(p) >= 3 and p.endswith("-")}
    assert families == set(DYNAMIC), f"dynamic prefixes in index.html {sorted(families)} vs DYNAMIC {sorted(DYNAMIC)}"
    assert all(DYNAMIC.values()), DYNAMIC
    # a prefix that is not a family (e.g. the "r" of r${flags}) is checked as a plain class
    plain = {p for p in prefixes if p not in families}
    assert all(len(p) < 3 or not p.endswith("-") for p in plain)


def test_defined_classes_ignores_decimals_in_values():
    css = ".a{margin:1.125rem .5em 0.25rem}.b-c{top:-.5px}.d\\.5{x:1}"
    assert defined_classes(css) == {"a", "b-c", "d.5"}
