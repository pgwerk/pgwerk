import json

SITE_URL = "https://pgwerk.github.io/pgwerk"
PYPI_URL = "https://pypi.org/project/pgwerk/"
GITHUB_URL = "https://github.com/pgwerk/pgwerk"

ORGANIZATION = {
    "@type": "Organization",
    "@id": f"{SITE_URL}/#organization",
    "name": "pgwerk",
    "url": SITE_URL,
    "sameAs": [GITHUB_URL, PYPI_URL],
}

SOFTWARE_APP = {
    "@context": "https://schema.org",
    "@type": "SoftwareApplication",
    "name": "pgwerk",
    "url": SITE_URL,
    "applicationCategory": "DeveloperApplication",
    "operatingSystem": "Linux, macOS, Windows",
    "programmingLanguage": "Python",
    "description": "Postgres-backed job queue for Python. Stores jobs as rows, dequeues with SELECT FOR UPDATE SKIP LOCKED.",
    "offers": {"@type": "Offer", "price": "0", "priceCurrency": "USD"},
    "downloadUrl": PYPI_URL,
    "codeRepository": GITHUB_URL,
    "license": "https://opensource.org/licenses/MIT",
    "publisher": ORGANIZATION,
}

WEBSITE = {
    "@context": "https://schema.org",
    "@type": "WebSite",
    "name": "pgwerk",
    "url": SITE_URL,
    "description": "Postgres-backed job queue for Python. Stores jobs as rows, dequeues with SELECT FOR UPDATE SKIP LOCKED.",
    "publisher": ORGANIZATION,
    "potentialAction": {
        "@type": "SearchAction",
        "target": f"{SITE_URL}/search/?q={{search_term_string}}",
        "query-input": "required name=search_term_string",
    },
}

PAGE_EXTRA_SCHEMAS = {
    "/": [SOFTWARE_APP, WEBSITE],
}


def _inject_scripts(html: str, schemas: list[dict]) -> str:
    tag = "<!-- JSON-LD Structured Data -->"
    if tag not in html:
        return html
    inserts = "\n".join(
        f'<script type="application/ld+json">\n{json.dumps(s, indent=2)}\n</script>'
        for s in schemas
    )
    return html.replace(tag, f"{tag}\n{inserts}", 1)


def on_post_page(output: str, page, **_kwargs) -> str:  # type: ignore[no-untyped-def]
    url = page.url or ""
    if not url.startswith("/"):
        url = f"/{url}"
    if not url.endswith("/"):
        url = f"{url}/"

    schemas = PAGE_EXTRA_SCHEMAS.get(url, [])
    if not schemas:
        return output

    return _inject_scripts(output, schemas)
