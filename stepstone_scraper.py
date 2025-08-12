#!/usr/bin/env python3
import asyncio
import csv
import logging
import os
import re
import sys
from datetime import datetime
from typing import List, Dict, Set, Optional
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# =============================
# Output/Env
# =============================
OUT_DIR = "/data" if os.path.isdir("/data") else os.getcwd()
os.makedirs(OUT_DIR, exist_ok=True)

LEAD_LIMIT = int(os.getenv("LEAD_LIMIT", "1000"))
PROXY_SERVER = os.getenv("PROXY_SERVER")
PROXY_USER   = os.getenv("PROXY_USER")
PROXY_PASS   = os.getenv("PROXY_PASS")

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
# Config (wie Overnight)
# =============================
SEARCH_PARAMS = [
    (kw, city, radius)
    for kw in [
        "pflegekraft", "pflegehilfskraft", "servicetechniker",
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
MIN_JOBS   = 7
MAX_JOBS   = 50
ACCESS_DENIED_LIMIT = 10

PROGRESS_FILE = os.path.join(OUT_DIR, "progress.txt")
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV   = os.path.join(OUT_DIR, f"stepstone_raw_leads_{ts}.csv")
FINAL_CSV = os.path.join(OUT_DIR, f"stepstone_leads_{ts}.csv")

# =============================
# Helpers
# =============================
def slug_city(text: str) -> str:
    repl = (("ä","ae"),("ö","oe"),("ü","ue"),("ß","ss"))
    s = text.strip().lower()
    for a,b in repl: s = s.replace(a,b)
    return re.sub(r"\s+", "-", s)

def build_search_url(keyword: str, city: str, radius: int, page_num: int) -> str:
    return f"https://www.stepstone.de/jobs/{keyword}/in-{slug_city(city)}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"

async def accept_all_cookies(page):
    try:
        await page.click('#ccmgt_explicit_accept', timeout=5000)
    except Exception:
        pass

async def is_access_denied(page) -> bool:
    try:
        txt = (await page.content()).lower()
        return "access denied" in txt or "permission to access" in txt
    except Exception:
        return False

async def get_job_count(page) -> int:
    try:
        el = await page.wait_for_selector('span.at-facet-header-total-results', timeout=12000)
        text = await el.inner_text()
        digits = re.sub(r"\D", "", text)
        return int(digits) if digits else 0
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
    # Mehrere Pattern – UIDs tauchen in Links/JSON auf
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

async def count_on_profile(context, url: str) -> int:
    """Zählt auf einer Firmenprofil-/CMP-Seite (Overnight-Standard)."""
    p = await context.new_page()
    try:
        await stealth_async(p)
        await p.goto(url, wait_until="domcontentloaded", timeout=30000)
        await accept_all_cookies(p)
        return await get_job_count(p)
    finally:
        await p.close()

async def count_on_companyuid(context, uid: str) -> int:
    """Zählt auf /jobs/?companyUid=... (Fallback)."""
    list_url = f"https://www.stepstone.de/jobs/?companyUid={uid}"
    p = await context.new_page()
    try:
        await stealth_async(p)
        await p.goto(list_url, wait_until="domcontentloaded", timeout=20000)
        await accept_all_cookies(p)
        return await get_job_count(p)
    finally:
        await p.close()

# =============================
# Main
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
        except Exception:
            start_index = 0

    async with async_playwright() as pw:
        # Browser (kleines Stabilitäts-Flag gegen HTTP/2-Macken)
        browser = await pw.chromium.launch(
            headless=True,
            args=["--no-sandbox", "--disable-http2"]
        )

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
                    await asyncio.sleep(5)
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

                        # === Titel & Firma (wie Overnight; die res-* Klassen sind vom Overnight-Beispiel) ===
                        title = await card.locator("[data-testid='job-item-title'] div.res-ewgtgq").inner_text()
                        company = await card.locator("span[data-at='job-item-company-name'] span.res-du9bhi").inner_text()
                        title = title.strip(); company = company.strip()
                        logger.info(f"📝 Gefunden: {title} bei {company}")

                        if company in seen_companies:
                            continue

                        # 1) Primärweg wie Overnight: Logo-Link → Firmenprofil zählen
                        profile_url = None
                        logo = card.locator("a[data-at='company-logo']").first
                        if await logo.count():
                            href = await logo.get_attribute("href")
                            if href:
                                profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                        job_count = 0
                        if profile_url:
                            try:
                                job_count = await count_on_profile(context, profile_url)
                            except Exception as e:
                                logger.debug(f"Profilzählung fehlgeschlagen: {e}")

                        # 2) Fallback: companyUid holen (direkt aus Karte oder aus Jobdetailseite)
                        if job_count == 0:
                            uid = None
                            # a) direkt aus Karte (Namenslink mit companyUid)
                            name_link = card.locator("[data-at='job-item-company-name'] a[href*='companyUid=']").first
                            if await name_link.count():
                                href = await name_link.get_attribute("href") or ""
                                m = re.search(r'companyUid=([0-9a-fA-F\-]{16,})', href)
                                if m: uid = m.group(1)

                            # b) sonst Jobdetailseite kurz öffnen und UID parsen
                            if not uid:
                                job_a = card.locator("[data-testid='job-item-title'] a").first
                                if await job_a.count():
                                    href = await job_a.get_attribute("href")
                                    if href:
                                        job_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"
                                        d = await context.new_page(); await stealth_async(d)
                                        try:
                                            await d.goto(job_url, wait_until="domcontentloaded", timeout=20000)
                                            await accept_all_cookies(d)
                                            html = await d.content()
                                            uid = extract_company_uid_from_html(html)
                                        except Exception:
                                            pass
                                        finally:
                                            await d.close()

                            if uid:
                                job_count = await count_on_companyuid(context, uid)
                                if job_count > 0:
                                    logger.info(f"🔗 Fallback via companyUid erfolgreich ({uid})")

                        logger.info(f"🔎 {company}: {job_count} Jobs")

                        if MIN_JOBS <= job_count <= MAX_JOBS:
                            seen_companies.add(company)
                            entry = {
                                'keyword': keyword,
                                'location': location,
                                'title': title,
                                'company': company,
                                'jobs': job_count,
                                'profile': profile_url or (f"https://www.stepstone.de/jobs/?companyUid={uid}" if job_count>0 and 'uid' in locals() and uid else "")
                            }
                            raw_leads.append(entry)
                            append_raw_row(entry)
                            total_hits += 1
                            logger.info(f"🚀 Lead {total_hits} gespeichert: {company} ({job_count})")

                    except Exception as e:
                        logger.error(f"❌ Fehler beim Auslesen Karte {i+1}: {e}")
                        continue

            with open(PROGRESS_FILE, 'w') as f:
                f.write(str(idx+1))

        await browser.close()

    # Dedupe & Save
    unique = {(r['company'], r['profile']): r for r in raw_leads}
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

if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("⚠️ Abbruch durch Benutzer")
        sys.exit(1)
