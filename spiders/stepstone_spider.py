import os, re, json
import scrapy
from urllib.parse import urljoin, urlparse, parse_qs
from scrapy_playwright.page import PageMethod
from ..items import LeadItem

# ---- Konfiguration (ENV > Defaults) ----
PAGE_LIMIT  = int(os.getenv("PAGE_LIMIT", "20"))
MIN_JOBS    = int(os.getenv("MIN_JOBS", "10"))
MAX_JOBS    = int(os.getenv("MAX_JOBS", "50"))
LEAD_LIMIT  = int(os.getenv("LEAD_LIMIT", "1000"))

SEARCH_PARAMS = [
    # (keyword, city, radius)
    ("gesundheits-und-krankenpfleger", "regensburg", 50),
    ("pflegehilfskraft", "würzburg", 50),
    ("servicetechniker", "stuttgart", 50),
    # ... ergänze deine Liste (oder über ENV laden)
]

def slug_city(text: str) -> str:
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    s = text.strip().lower()
    for a,b in repl: s = s.replace(a,b)
    return re.sub(r"\s+", "-", s)

def build_search_url(keyword: str, city: str, radius: int, page_num: int) -> str:
    return f"https://www.stepstone.de/jobs/{keyword}/in-{slug_city(city)}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"

def extract_company_uid_from_html(html: str):
    pats = [
        r'companyUid=([0-9a-fA-F\-]{16,})',
        r'"companyUid"\s*:\s*"([0-9a-fA-F\-]{16,})"',
        r"data-company-uid=['\"]([0-9a-fA-F\-]{16,})['\"]",
    ]
    for pat in pats:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)

def parse_total_from_html(html: str) -> int:
    # primär: bekannte JSON-Felder
    for pat in [r'"totalResultCount"\s*:\s*(\d+)', r'"totalResults"\s*:\s*(\d+)',
                r'"resultCount"\s*:\s*(\d+)', r'"totalJobs"\s*:\s*(\d+)']:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return int(m.group(1))
    # fallback: Zahl nahe "Ergebnisse"/"Treffer"
    m = re.search(r'(Ergebnisse|Treffer)[^0-9]{0,40}(\d[\d\.]*)', html, flags=re.IGNORECASE)
    return int(re.sub(r'\D', '', m.group(2))) if m else 0

class StepstoneSpider(scrapy.Spider):
    name = "stepstone"
    custom_settings = {
        "PLAYWRIGHT_PROCESS_REQUEST_HEADERS": lambda _, __: {},
    }

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.seen_companies = set()
        self.total_hits = 0

    def start_requests(self):
        for keyword, city, radius in SEARCH_PARAMS:
            for p in range(1, PAGE_LIMIT + 1):
                url = build_search_url(keyword, city, radius, p)
                yield scrapy.Request(
                    url,
                    meta={
                        "playwright": True,
                        # Cookie-Banner wegklicken
                        "playwright_page_methods": [
                            PageMethod("wait_for_load_state", "domcontentloaded"),
                            PageMethod("click", "#ccmgt_explicit_accept", timeout=3000, strict=False),
                        ],
                        "kw": keyword,
                        "city": city,
                    },
                    callback=self.parse_search,
                    errback=self.err_search,
                    dont_filter=True,
                )

    async def parse_search(self, response):
        if self.total_hits >= LEAD_LIMIT:
            return

        html = response.text.lower()
        if "access denied" in html or "permission to access" in html:
            self.logger.warning("Access denied – überspringe: %s", response.url)
            return

        # Jobkarten
        cards = response.css("article[data-at='job-item']")
        if not cards:
            # Seite leer → vermutlich Ende der Pagination für diesen Query
            return

        for card in cards:
            if self.total_hits >= LEAD_LIMIT:
                return

            # Titel
            title = card.css("[data-testid='job-item-title'] div::text").get()
            if not title:
                title = card.css("[data-testid='job-item-title'] *::text").get()
            if title:
                title = title.strip()

            # Firma
            company = card.css("span[data-at='job-item-company-name'] span::text").get()
            if not company:
                company = card.css("span[data-at='job-item-company-name'] *::text").get()
            if company:
                company = company.strip()

            if not company or company in self.seen_companies:
                continue

            # Firmenprofil (Logo-Link)
            profile_url = card.css("a[data-at='company-logo']::attr(href)").get()
            if profile_url:
                profile_url = response.urljoin(profile_url)

            # companyUid aus Karte
            uid_link = card.css("[data-at='job-item-company-name'] a[href*='companyUid=']::attr(href)").get()
            current_uid = None
            if uid_link:
                m = re.search(r'companyUid=([0-9a-fA-F\-]{16,})', uid_link)
                if m:
                    current_uid = m.group(1)

            # Wenn keine UID vorhanden, kurze Detailseite öffnen, um UID zu finden
            if not current_uid:
                job_href = card.css("[data-testid='job-item-title'] a::attr(href)").get()
                if job_href:
                    job_url = response.urljoin(job_href)
                    yield scrapy.Request(
                        job_url,
                        meta={"playwright": True, "kw": response.meta["kw"], "city": response.meta["city"],
                              "title": title, "company": company, "profile_url": profile_url},
                        callback=self.parse_uid_from_detail,
                        errback=self.err_search,
                        priority=10,
                    )
                    continue

            # Wir haben UID oder Profil – jetzt auf Zählerseite
            if profile_url:
                yield scrapy.Request(
                    profile_url,
                    meta={"playwright": True, "kw": response.meta["kw"], "city": response.meta["city"],
                          "title": title, "company": company, "profile_url": profile_url},
                    callback=self.parse_count_from_profile,
                    errback=self.err_search,
                    priority=5,
                )
            elif current_uid:
                list_url = f"https://www.stepstone.de/jobs/?companyUid={current_uid}"
                yield scrapy.Request(
                    list_url,
                    meta={"playwright": True, "kw": response.meta["kw"], "city": response.meta["city"],
                          "title": title, "company": company, "profile_url": list_url},
                    callback=self.parse_count_from_list,
                    errback=self.err_search,
                    priority=5,
                )

    async def parse_uid_from_detail(self, response):
        html = response.text
        uid = extract_company_uid_from_html(html)
        company = response.meta["company"]
        if not uid:
            # Fallback direkt zählen, falls Detailseite Gesamtzahl zeigt (selten)
            total = parse_total_from_html(html)
            return await self._maybe_yield(company, response.meta["title"], response.meta["kw"],
                                           response.meta["city"], total, response.url)

        list_url = f"https://www.stepstone.de/jobs/?companyUid={uid}"
        yield scrapy.Request(
            list_url,
            meta={"playwright": True, **response.meta, "profile_url": list_url},
            callback=self.parse_count_from_list,
            errback=self.err_search,
            priority=5,
        )

    async def parse_count_from_profile(self, response):
        total = parse_total_from_html(response.text)
        return await self._maybe_yield(response.meta["company"], response.meta["title"],
                                       response.meta["kw"], response.meta["city"],
                                       total, response.meta.get("profile_url") or response.url)

    async def parse_count_from_list(self, response):
        total = parse_total_from_html(response.text)
        return await self._maybe_yield(response.meta["company"], response.meta["title"],
                                       response.meta["kw"], response.meta["city"],
                                       total, response.meta.get("profile_url") or response.url)

    async def _maybe_yield(self, company, title, kw, city, total, profile_url):
        if not company or company in self.seen_companies:
            return
        self.logger.info("Company %s: %s Jobs", company, total)
        if MIN_JOBS <= total <= MAX_JOBS:
            self.seen_companies.add(company)
            self.total_hits += 1
            yield LeadItem(
                keyword=kw, location=city, title=title or "",
                company=company, jobs=total, profile=profile_url
            )

    def err_search(self, failure):
        self.logger.warning("Request failed: %s", failure.request.url)
