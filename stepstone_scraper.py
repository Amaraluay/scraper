#!/usr/bin/env python3
import asyncio
import csv
import logging
import os
import random
import re
import sys
from datetime import datetime
from typing import List, Dict, Set, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# =============================
# Output-/Env-Setup (Render-friendly)
# =============================
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

# Proxy NUR nutzen, wenn gesetzt (keine Defaults!)
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USER   = os.getenv("PROXY_USER")
PROXY_PASS   = os.getenv("PROXY_PASS")

LEAD_LIMIT   = int(os.getenv("LEAD_LIMIT", "1000"))

# =============================
# Logging
# =============================
LOG_FILE = os.path.join(OUT_DIR, "stepstone_scraper.log")
logger = logging.getLogger("StepstoneScraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler(LOG_FILE); fh.setFormatter(fmt); logger.addHandler(fh)

# =============================
# Config – wie Overnight
# =============================
SEARCH_PARAMS = [
    (kw, city, radius)
    for kw in [
        "gesundheits-und-krankenpfleger", "pflegehilfskraft", "servicetechniker",
        "aussendienst", "produktionsmitarbeiter", "maschinen-und-anlagenfuehrer",
        "fertigungsmitarbeiter", "kundenberater", "kundenservice", "kundendienstberater"
    ]
    for city, radius in [
        ("regensburg", 50), ("würzburg", 50), ("freiburg", 50), ("ingolstadt", 50),
        ("trier", 50), ("saarbrücken", 50), ("konstanz", 30), ("nürnberg", 50),
        ("passau", 50), ("ulm", 50), ("muenchen", 50), ("frankfurt", 50),
        ("augsburg", 50), ("stuttgart", 50), ("mannheim", 50), ("karlsruhe", 50),
        ("baden baden", 50), ("baden", 50)
    ]
]

PAGE_LIMIT = 20
MIN_JOBS   = 10
MAX_JOBS   = 50
ACCESS_DENIED_LIMIT = 10

PROGRESS_FILE = os.path.join(OUT_DIR, "progress.txt")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV   = os.path.join(OUT_DIR, f"stepstone_raw_leads_{ts}.csv")
FINAL_CSV = os.path.join(OUT_DIR, f"stepstone_leads_{ts}.csv")

# =============================
# Helpers (wie Overnight + Fallbacks)
# =============================
def slug_city(text: str) -> str:
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    s = text.strip().lower()
    for a,b in repl: s = s.replace(a,b)
    return re.sub(r"\s+", "-", s)

def slug_company_for_cmp(name: str) -> str:
    """Slug für /cmp/de/<slug>--<id>/jobs"""
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    s = name.strip().lower()
    for a,b in repl: s = s.replace(a,b)
    s = re.sub(r"[^a-z0-9\s\-/&\.]", "", s)
    s = s.replace("&", "und").replace("/", "-").replace(".", "")
    s = re.sub(r"[\s\-]+", "-", s).strip("-")
    return s

def build_search_url(keyword: str, city: str, radius: int, page_num: int) -> str:
    return f"https://www.stepstone.de/jobs/{keyword}/in-{slug_city(city)}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"

async def accept_all_cookies(page):
    try:
        await page.click('#ccmgt_explicit_accept', timeout=5000)
        logger.debug("✅ Cookies akzeptiert")
    except PlaywrightTimeoutError:
        logger.debug("⚠️ Kein Cookie-Banner oder bereits akzeptiert")
    except Exception:
        pass

async def is_access_denied(page) -> bool:
    try:
        txt = (await page.content()).lower()
        return "access denied" in txt or "permission to access" in txt
    except:
        return False

async def get_job_count(page) -> int:
    try:
        el = await page.wait_for_selector('span.at-facet-header-total-results', timeout=10000)
        text = await el.inner_text()
        return int(re.sub(r"\D", "", text))
    except Exception:
        return 0

def ensure_raw_header():
    if not os.path.exists(RAW_CSV):
        with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
            writer.writeheader()

def append_raw_row(row: Dict):
    with open(RAW_CSV, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=['keyword','location','title','company','jobs','profile'])
        writer.writerow(row)

def extract_company_uid_from_html(html: str) -> Optional[str]:
    """Extrahiere companyUid aus Jobdetail-HTML."""
    pats = [
        r'companyUid=([0-9a-fA-F\-]{16,})',
        r'"companyUid"\s*:\s*"([0-9a-fA-F\-]{16,})"',
        r"data-company-uid=['\"]([0-9a-fA-F\-]{16,})['\"]",
    ]
    for pat in pats:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None

def extract_company_id_from_html(html: str) -> Optional[str]:
    """Finde eine companyId / employerId oder eine cmp-URL mit --<id> im HTML."""
    patterns = [
        r'/cmp/de/[^"\']*--(\d+)/jobs',
        r'"companyId"\s*:\s*"?(\d+)"?',
        r'"employerId"\s*:\s*"?(\d+)"?',
        r"data-company-id=['\"](\d+)['\"]",
    ]
    for pat in patterns:
        m = re.search(pat, html, flags=re.IGNORECASE)
        if m:
            return m.group(1)
    return None

async def count_on_url(browser, context, url: str) -> int:
    """Zählt Jobs auf einer Listing-/Profilseite. Bei HTTP2/Proxy-Problemen zweiter Versuch ohne Proxy."""
    async def _count_in_ctx(ctx):
        p = await ctx.new_page()
        try:
            await stealth_async(p)
            await p.goto(url, wait_until="commit", timeout=15000)   # schneller & weniger zickig
            await accept_all_cookies(p)
            return await get_job_count(p)
        finally:
            await p.close()

    # Versuch im aktuellen Context
    try:
        return await _count_in_ctx(context)
    except Exception as e:
        msg = str(e)
        logger.warning(f"⚠️ count_on_url Fehler {url}: {e}")
        # Bei HTTP2-/Proxy-Timeout → no-proxy versuchen (nur wenn Proxy aktiv)
        if PROXY_SERVER and ("HTTP2" in msg.upper() or "TIMEOUT" in msg.upper()):
            logger.info("↪️ Retry ohne Proxy-Context")
            no_proxy_ctx = await browser.new_context(ignore_https_errors=True)
            try:
                return await _count_in_ctx(no_proxy_ctx)
            finally:
                await no_proxy_ctx.close()
        return 0

async def count_jobs_smart(browser, context, company_name: str, profile_url: Optional[str], job_detail_url: Optional[str]) -> int:
    """
    1) Wenn profile_url da: dort zählen.
    2) Wenn 0/Fehler und job_detail_url da: Detailseite öffnen, companyUid suchen,
       dann /jobs/?companyUid=... aufrufen und zählen.
    3) Wenn keine UID, aber companyId vorhanden: /cmp/de/<slug>--<id>/jobs bauen und zählen.
    """
    # 1) profil versuchen
    if profile_url:
        c = await count_on_url(browser, context, profile_url)
        if c > 0:
            return c

    # 2) UID aus Jobdetail
    if job_detail_url:
        d = await context.new_page()
        await stealth_async(d)
        try:
            await d.goto(job_detail_url, wait_until="commit", timeout=15000)
            await accept_all_cookies(d)
            html = await d.content()
            uid = extract_company_uid_from_html(html)
            if uid:
                listing_url = f"https://www.stepstone.de/jobs/?companyUid={uid}"
                c = await count_on_url(browser, context, listing_url)
                if c > 0:
                    return c
            # 3) companyId → cmp/de/<slug>--<id>/jobs
            comp_id = extract_company_id_from_html(html)
            if comp_id:
                slug = slug_company_for_cmp(company_name)
                cmp_url = f"https://www.stepstone.de/cmp/de/{slug}--{comp_id}/jobs"
                c = await count_on_url(browser, context, cmp_url)
                if c > 0:
                    return c
        finally:
            await d.close()

    return 0

# =============================
# Main scraping (Overnight-Flow + smarter Fallback)
# =============================
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    access_denied_count = 0
    total_hits = 0

    ensure_raw_header()

    # Progress lesen
    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        try:
            with open(PROGRESS_FILE, 'r') as f:
                start_index = int(f.read().strip())
        except:
            start_index = 0

    async with async_playwright() as pw:
        # Browser mit HTTP/2-Workaround starten
        launch_args = ["--no-sandbox", "--disable-http2"]
        browser = await pw.chromium.launch(headless=True, args=launch_args)

        # Context: nur Proxy, wenn gesetzt
        ctx_kwargs = {"ignore_https_errors": True}
        if PROXY_SERVER:
            ctx_kwargs["proxy"] = {
                "server": PROXY_SERVER,
                **({"username": PROXY_USER} if PROXY_USER else {}),
                **({"password": PROXY_PASS} if PROXY_PASS else {}),
            }
        context = await browser.new_context(**ctx_kwargs)

        page = await context.new_page()
        await stealth_async(page)

        for idx, (keyword, location, radius) in enumerate(SEARCH_PARAMS[start_index:], start=start_index):
            logger.info(f"🚀 Starte Suche {idx+1}/{len(SEARCH_PARAMS)}: {keyword} in {location}")

            for page_index in range(1, PAGE_LIMIT + 1):
                # Lead-Limit prüfen
                if total_hits >= LEAD_LIMIT:
                    logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Stoppe.")
                    with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                    await browser.close()
                    return

                url = build_search_url(keyword, location, radius, page_index)
                logger.info(f"🔍 Seite {page_index}: {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await accept_all_cookies(page)
                except Exception as e:
                    logger.warning(f"⚠️ Fehler bei {url}: {e}")
                    break

                if await is_access_denied(page):
                    access_denied_count += 1
                    logger.warning(f"⚠️ Access denied auf {url} (insg. {access_denied_count})")
                    if access_denied_count >= ACCESS_DENIED_LIMIT:
                        logger.error("❌ Zu viele Access Denied – abbrechen")
                        with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                        await browser.close()
                        return
                    await asyncio.sleep(random.randint(5, 10))
                    continue

                cards = page.locator("article[data-at='job-item']")
                count = await cards.count()
                logger.info(f"📦 Gefundene Jobkarten: {count}")
                if count == 0:
                    break

                for i in range(count):
                    if total_hits >= LEAD_LIMIT:
                        logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Stoppe.")
                        with open(PROGRESS_FILE, 'w') as f: f.write(str(idx))
                        await browser.close()
                        return

                    try:
                        card = cards.nth(i)
                        # Titel & Firma – exakt wie im Overnight-Beispiel (Achtung: Klassen können variieren)
                        title = await card.locator("[data-testid='job-item-title'] div.res-ewgtgq").inner_text()
                        company = await card.locator("span[data-at='job-item-company-name'] span.res-du9bhi").inner_text()
                        title = title.strip(); company = company.strip()
                        logger.info(f"📝 Gefunden: {title} bei {company}")

                        if company in seen_companies:
                            logger.debug(f"↩️ Überspringe bereits erfasste Firma: {company}")
                            continue

                        # Firmenprofil über Firmenlogo öffnen – wie Overnight
                        href = await card.locator("a[data-at='company-logo']").get_attribute('href')
                        profile_url = href if href and href.startswith("http") else (f"https://www.stepstone.de{href}" if href else None)

                        # Jobdetail-URL (für UID/ID-Fallback)
                        job_link = card.locator("[data-testid='job-item-title'] a").first
                        job_href = await job_link.get_attribute("href") if await job_link.count() else None
                        job_detail_url = job_href if job_href and job_href.startswith("http") else (f"https://www.stepstone.de{job_href}" if job_href else None)

                        # Smarter Zähler (Profil → UID-Listing → CMP-<id>)
                        job_count = await count_jobs_smart(browser, context, company, profile_url, job_detail_url)
                        logger.info(f"🔎 {company}: {job_count} Jobs")

                        if MIN_JOBS <= job_count <= MAX_JOBS:
                            seen_companies.add(company)
                            entry = {
                                'keyword': keyword,
                                'location': location,
                                'title': title,
                                'company': company,
                                'jobs': job_count,
                                'profile': profile_url or (f"https://www.stepstone.de/jobs/?companyUid={company}" if job_detail_url else "")
                            }
                            raw_leads.append(entry)
                            append_raw_row(entry)  # Live-Append
                            total_hits += 1
                            logger.info(f"🚀 Lead {total_hits} gespeichert: {company} ({job_count})")

                    except Exception as e:
                        logger.error(f"❌ Fehler beim Auslesen Karte {i+1}: {e}")
                        continue

            # Fortschritt sichern (nächste Stadt/Keyword)
            with open(PROGRESS_FILE, 'w') as f:
                f.write(str(idx+1))

        await browser.close()

    # Dedupe & Save
    unique = { (r['company'], r['profile']): r for r in raw_leads }
    leads = list(unique.values())
    keys = ['keyword','location','title','company','jobs','profile']

    with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(raw_leads)
    logger.info(f"💾 Rohdaten → {RAW_CSV}")

    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(leads)
    logger.info(f"🎉 Fertig: {len(leads)} eindeutige Leads → {FINAL_CSV}")

# =============================
# Main
# =============================
if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("⚠️ Abbruch durch Benutzer")
        sys.exit(1)
