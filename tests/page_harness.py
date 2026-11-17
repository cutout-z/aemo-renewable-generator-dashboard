"""Offline page harness: static CSS-class extraction and a Playwright page loader.

Test-only helper (per-repo copy; the AEMO repos share no package). Nothing here touches the network:
the page's own files come from the repo checkout, PapaParse from a vendored copy, SheetJS is a stub, the
shared nav.js is an empty script (the real one is tested in aemo-dashboards), and every other request is
aborted and recorded so a test can fail on it.
"""

import contextlib
import mimetypes
import re
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
VENDOR = Path(__file__).resolve().parent / "fixtures" / "vendor"
ORIGIN = "http://aemo.test"
PAPAPARSE_URL = "https://cdn.jsdelivr.net/npm/papaparse@5.4.1/papaparse.min.js"
XLSX_URL = "https://cdn.jsdelivr.net/npm/xlsx@0.18.5/dist/xlsx.full.min.js"
NAV_URL = "https://cutout-z.github.io/aemo-dashboards/nav.js"
# Only the members the page touches. Loading never calls them; an export click does (and is counted).
XLSX_STUB = (
    "window.XLSX={utils:{book_new(){return {}},aoa_to_sheet(){return {}},book_append_sheet(){}},"
    "writeFile(){window.__xlsxWrites=(window.__xlsxWrites||0)+1}};"
)


# --- static: classes used by the page vs classes the compiled CSS defines -------------------------

_ATTR = re.compile(r"""\bclass=(?:"([^"]*)"|'([^']*)')""")
_CLASSLIST = re.compile(r"""classList\.(?:add|remove|toggle|contains|replace)\(([^)]*)\)""")
_CLASSNAME = re.compile(r"""\bclassName\s*\+?=\s*(?:'([^']*)'|"([^"]*)"|`([^`]*)`)""")
_QUOTED = re.compile(r"""'([^']*)'|"([^"]*)"|`([^`]*)`""")
_INTERP = re.compile(r"\$\{(.*?)\}", re.S)
_CSS_ESC = r"\\(?:[0-9a-fA-F]{1,6} ?|.)"
# A class token starts with a letter, "-", "_" or an escape and does not follow a digit, so the
# decimal in a value ("1.125rem", ".5em") is not a class.
_CSS_CLASS = re.compile(
    r"(?<![0-9])\.((?:" + _CSS_ESC + r"|[A-Za-z_-])(?:" + _CSS_ESC + r"|[A-Za-z0-9_-])*)"
)
_CSS_NOISE = re.compile(r"""/\*.*?\*/|url\([^)]*\)|"[^"]*"|'[^']*'""", re.S)
_CSS_UNESC = re.compile(r"\\(?:([0-9a-fA-F]{1,6}) ?|(.))")


def _tokens(text):
    return [t for t in text.split() if t]


def _attr_classes(value):
    """(classes, prefixes) in one class="..." value.

    Literal tokens are classes; string literals inside ${...} are classes too. A token glued to a
    ${...} ("seq-${i}") is a dynamic prefix: the page builds the rest at run time.
    """
    classes, prefixes = set(), set()
    for expr in _INTERP.findall(value):
        for m in _QUOTED.finditer(expr):
            classes.update(_tokens(next(g for g in m.groups() if g is not None)))
    for token in _tokens(_INTERP.sub("\x00", value)):
        if "\x00" in token:
            head = token.split("\x00", 1)[0]
            if head:
                prefixes.add(head)
        elif "{" not in token and "}" not in token:
            classes.add(token)
    return classes, prefixes


_CLASS_OK = re.compile(r"[A-Za-z0-9_:./\[\]%#(),!&>+~='\"-]+")


def used_classes(html):
    """(classes, prefixes) the page can put on an element: markup, JS template strings, classList calls."""
    found, prefixes = set(), set()
    for m in _ATTR.finditer(html):
        c, p = _attr_classes(m.group(1) if m.group(1) is not None else m.group(2))
        found |= c
        prefixes |= p
    for m in _CLASSLIST.finditer(html):
        for q in _QUOTED.finditer(m.group(1)):
            found.update(_tokens(next(g for g in q.groups() if g is not None)))
    for m in _CLASSNAME.finditer(html):
        found.update(_tokens(next(g for g in m.groups() if g is not None)))
    return {c for c in found if _CLASS_OK.fullmatch(c)}, prefixes


def defined_classes(css):
    """Class names with a selector in the compiled stylesheet (CSS escapes undone)."""
    def undo(m):
        return chr(int(m.group(1), 16)) if m.group(1) else m.group(2)

    return {_CSS_UNESC.sub(undo, m.group(1)) for m in _CSS_CLASS.finditer(_CSS_NOISE.sub(" ", css))}


def inline_css(html):
    return "\n".join(re.findall(r"<style[^>]*>(.*?)</style>", html, re.S | re.I))


def missing_classes(html, css, dynamic=None):
    """Used classes that neither app.css nor the page's inline <style> defines.

    `dynamic` maps each dynamic prefix the page builds ("seq-" for `seq-${i}`) to the concrete classes the
    JS can produce from it; every one must be defined. A prefix that is not a family (under 3 characters
    or not ending in "-": the "r" of `r${flags}`) is a plain class glued to a ${...}, so it must be defined
    itself. A family prefix with no entry in `dynamic` is reported as "<prefix>*": enumerate it.
    """
    dynamic = dynamic or {}
    used, prefixes = used_classes(html)
    defined = defined_classes(css + "\n" + inline_css(html))
    missing = set()
    for p in prefixes:
        if len(p) >= 3 and p.endswith("-"):
            if p in dynamic:
                used |= set(dynamic[p])
            else:
                missing.add(p + "*")
        else:
            used.add(p)
    return missing | (used - defined)


# --- rendered page ----------------------------------------------------------------------------------


class PageLog:
    def __init__(self):
        self.page_errors = []
        self.console_errors = []
        self.unexpected = []  # requests the page made that the harness refused
        self.served = []  # own files served, as URL paths


def _own_file(url_path):
    rel = url_path.lstrip("/") or "index.html"
    target = (ROOT / rel).resolve()
    try:
        target.relative_to(ROOT)
    except ValueError:
        return None
    return target if target.is_file() else None


def _install_routes(context, log, overrides):
    def handle(route):
        url = route.request.url
        if url in overrides:
            body, ctype = overrides[url]
            return route.fulfill(status=200, body=body, content_type=ctype)
        if url == PAPAPARSE_URL:
            return route.fulfill(
                status=200, body=(VENDOR / "papaparse.min.js").read_bytes(), content_type="application/javascript"
            )
        if url == XLSX_URL:
            return route.fulfill(status=200, body=XLSX_STUB, content_type="application/javascript")
        if url == NAV_URL:
            return route.fulfill(status=200, body="", content_type="application/javascript")
        if url.startswith(ORIGIN + "/"):
            path = url[len(ORIGIN):].split("?", 1)[0].split("#", 1)[0]
            target = _own_file(path)
            if target is not None:
                log.served.append(path)
                ctype = mimetypes.guess_type(target.name)[0] or "application/octet-stream"
                return route.fulfill(status=200, body=target.read_bytes(), content_type=ctype)
        log.unexpected.append(f"{route.request.method} {url}")
        return route.abort()

    context.route("**/*", handle)


@contextlib.contextmanager
def loaded_page(width=1440, height=900, overrides=None, wait_for=None, settle_ms=0):
    """Open index.html offline in headless chromium. Yields (page, PageLog).

    `overrides` maps a full URL to (body, content_type) to replace one own file with a fixture.
    `wait_for` is a JS expression that becomes truthy once the page has rendered its data.
    """
    from playwright.sync_api import sync_playwright

    log = PageLog()
    with sync_playwright() as p:
        browser = p.chromium.launch()
        try:
            context = browser.new_context(viewport={"width": width, "height": height}, service_workers="block")
            _install_routes(context, log, overrides or {})
            page = context.new_page()
            page.on("pageerror", lambda e: log.page_errors.append(str(e)))
            page.on(
                "console",
                lambda m: log.console_errors.append(m.text) if m.type == "error" else None,
            )
            page.goto(ORIGIN + "/index.html", wait_until="load")
            if wait_for:
                page.wait_for_function(wait_for, timeout=20000)
            if settle_ms:
                page.wait_for_timeout(settle_ms)
            yield page, log
        finally:
            browser.close()


def assert_clean(log):
    problems = []
    if log.page_errors:
        problems.append(f"page errors: {log.page_errors}")
    if log.console_errors:
        problems.append(f"console errors: {log.console_errors}")
    if log.unexpected:
        problems.append(f"unexpected requests: {log.unexpected}")
    assert not problems, "; ".join(problems)
