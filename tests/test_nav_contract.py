"""Static guard on the page's own nav contract.

The offline smoke serves an empty script for the shared nav.js, so a removed tag or a mistyped
data-current would pass every rendered test. This pins the tag the page sends: exactly one script
with the shared nav.js URL, and a data-current key the shared nav knows (tests/fixtures/nav_keys.txt).
"""

import re

from page_harness import NAV_URL, ROOT

HTML = (ROOT / "index.html").read_text()
KEYS = {
    line.strip()
    for line in (ROOT / "tests" / "fixtures" / "nav_keys.txt").read_text().splitlines()
    if line.strip() and not line.lstrip().startswith("#")
}


def _script_tags(html):
    return re.findall(r"<script\b[^>]*>", html, re.I)


def _attr(tag, name):
    m = re.search(rf"""\b{name}\s*=\s*(?:"([^"]*)"|'([^']*)')""", tag, re.I)
    return None if not m else (m.group(1) if m.group(1) is not None else m.group(2))


def test_exactly_one_shared_nav_script_tag():
    tags = [t for t in _script_tags(HTML) if _attr(t, "src") == NAV_URL]
    assert len(tags) == 1, f"expected one <script src={NAV_URL}>, found {len(tags)}"
    other = [t for t in _script_tags(HTML) if "nav.js" in (_attr(t, "src") or "") and _attr(t, "src") != NAV_URL]
    assert not other, f"a nav.js script with a different URL: {other}"


def test_data_current_is_a_key_the_shared_nav_knows():
    (tag,) = [t for t in _script_tags(HTML) if _attr(t, "src") == NAV_URL]
    current = _attr(tag, "data-current")
    assert current in KEYS, f"data-current={current!r} is not one of the nav keys {sorted(KEYS)}"
