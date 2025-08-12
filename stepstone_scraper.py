#!/usr/bin/env python3
import asyncio
import csv
import logging
import os
import random
import re
import sys
from datetime import datetime
from typing import List, Dict, Set
from playwright.async_api import async_playwright, TimeoutError as PlaywrightTimeoutError
from playwright_stealth import stealth_async

# -----------------------------
# Config
# -----------------------------
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
        ("passau", 50), ("ulm", 50), ("muenchen", 50), ("frankfurt", 50), ("augsburg", 50),
        ("stuttgart", 50), ("mannheim", 50), ("karlsruhe", 50), ("baden-baden", 50), ("baden", 50)
    ]
]

PAGE_LIMIT = 20
MIN_JOBS = 8
MAX_JOBS = 45
MAX_DENIED = 5
LEAD_LIMIT = 1000  # Limit der Leads

PROXY_SERVER = "http://de.decodo.com:20001"
PROXY_USER = "sp2ji26uar"
PROXY_PASS = "l1+i6y9qSUFduqv3Sv"

PROGRESS_FILE = "progress.txt"
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
RAW_CSV = f"stepstone_raw_leads_{ts}.csv"
FINAL_CSV = f"stepstone_leads_{ts}.csv"

# -----------------------------
# Logging setup
# -----------------------------
logger = logging.getLogger("StepstoneScraper")
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
ch = logging.StreamHandler(); ch.setFormatter(fmt); logger.addHandler(ch)
fh = logging.FileHandler("stepstone_scraper.log"); fh.setFormatter(fmt); logger.addHandler(fh)

# -----------------------------
# Helpers
# -----------------------------
async def accept_all_cookies(page):
    try:
        await page.click('#ccmgt_explicit_accept', timeout=3000)
        logger.debug("✅ Cookies akzeptiert")
    except:
        pass

async def is_access_denied(page) -> bool:
    txt = (await page.content()).lower()
    return "access denied" in txt or "permission to access" in txt

async def get_job_count(page) -> int:
    try:
        el = await page.wait_for_selector('span.at-facet-header-total-results', timeout=10000)
        text = await el.inner_text()
        return int(re.sub(r"\D", "", text))
    except:
        return 0

async def fallback_job_search(context, company) -> int:
    fallback_url = f"https://www.stepstone.de/jobs/in-deutschland?keywords={company.replace(' ', '%20')}"
    fallback_page = await context.new_page()
    try:
        await fallback_page.goto(fallback_url, wait_until="domcontentloaded", timeout=30000)
        await accept_all_cookies(fallback_page)
        count = await get_job_count(fallback_page)
        logger.info(f"🔁 Fallback-Suche für {company}: {count} Jobs")
        return count
    except Exception as e:
        logger.error(f"❌ Fallback-Fehler für {company}: {e}")
        return 0
    finally:
        await fallback_page.close()

# -----------------------------
# Scraper
# -----------------------------
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    access_denied_count = 0
    total_hits = 0
    reached_limit = False

    start_index = 0
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE, 'r') as f:
            start_index = int(f.read().strip())

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=True, args=["--no-sandbox"])
        context = await browser.new_context(
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USER,
                "password": PROXY_PASS
            }
        )
        page = await context.new_page()
        await stealth_async(page)

        for idx, (keyword, location, radius) in enumerate(SEARCH_PARAMS[start_index:], start=start_index):
            hits_this_search = 0
            logger.info(f"🚀 Starte Suche {idx+1}/{len(SEARCH_PARAMS)}: {keyword} in {location}")

            for page_num in range(1, PAGE_LIMIT + 1):
                url = f"https://www.stepstone.de/jobs/{keyword}/in-{location}?radius={radius}&page={page_num}&searchOrigin=Resultlist_top-search"
                logger.info(f"🔍 Seite {page_num}: {url}")
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=30000)
                    await accept_all_cookies(page)
                except Exception as e:
                    logger.warning(f"⚠️ Fehler bei {url}: {e}")
                    continue

                if await is_access_denied(page):
                    access_denied_count += 1
                    logger.warning(f"🚫 Access denied (#{access_denied_count})")
                    if access_denied_count >= MAX_DENIED:
                        logger.warning("💤 Zu viele Access Denied – Warte 5 Minuten")
                        with open(PROGRESS_FILE, 'w') as f:
                            f.write(str(idx))
                        await asyncio.sleep(300)
                        return await scrape()
                    await asyncio.sleep(5)
                    continue

                cards = page.locator("article[data-at='job-item']")
                count = await cards.count()
                logger.info(f"📦 Gefundene Jobkarten: {count}")
                if count == 0:
                    break

                for i in range(count):
                    try:
                        card = cards.nth(i)
                        title = await card.locator("[data-testid='job-item-title']").locator("a, div").first.inner_text()
                        company = await card.locator("[data-at='job-item-company-name']").locator("a, span").first.inner_text()

                        if company in seen_companies:
                            continue

                        # Firmenprofil-Link
                        profile_url = None
                        logo_link = card.locator("a[data-at='company-logo']").first
                        if await logo_link.count():
                            href = await logo_link.get_attribute("href")
                            if href:
                                profile_url = href if href.startswith("http") else f"https://www.stepstone.de{href}"

                        if not profile_url:
                            job_link_el = card.locator("[data-testid='job-item-title'] a").first
                            job_href = await job_link_el.get_attribute("href") if await job_link_el.count() else None
                            if job_href:
                                job_url = job_href if job_href.startswith("http") else f"https://www.stepstone.de{job_href}"
                                detail_page = await context.new_page()
                                await stealth_async(detail_page)
                                try:
                                    await detail_page.goto(job_url, wait_until="domcontentloaded", timeout=30000)
                                    await accept_all_cookies(detail_page)
                                    try:
                                        company = await detail_page.locator("[data-at='company-name'], h3:has(a)").first.inner_text()
                                    except:
                                        pass
                                    try:
                                        comp_link = detail_page.locator("a[href*='/cmp/']")
                                        if await comp_link.count():
                                            profile_url = await comp_link.first.get_attribute("href")
                                    except:
                                        pass
                                finally:
                                    await detail_page.close()

                        # Falls kein Profil-URL → Fallback
                        job_count = 0
                        if profile_url:
                            profile_url = profile_url if profile_url.startswith("http") else f"https://www.stepstone.de{profile_url}"
                            prof_page = await context.new_page()
                            await stealth_async(prof_page)
                            try:
                                await prof_page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                                await accept_all_cookies(prof_page)
                                job_count = await get_job_count(prof_page)
                                if job_count == 0:
                                    job_count = await fallback_job_search(context, company)
                            finally:
                                await prof_page.close()
                        else:
                            job_count = await fallback_job_search(context, company)

                        logger.info(f"🔎 {company}: {job_count} Jobs")

                        if MIN_JOBS <= job_count <= MAX_JOBS:
                            seen_companies.add(company)
                            lead = {
                                "keyword": keyword,
                                "location": location,
                                "title": title.strip(),
                                "company": company.strip(),
                                "jobs": job_count,
                                "profile": profile_url or ""
                            }
                            raw_leads.append(lead)
                            total_hits += 1
                            hits_this_search += 1
                            logger.info(f"📥 Lead gespeichert (#{total_hits} gesamt, {hits_this_search} in aktueller Suche) → {company} [{job_count}]")

                            if total_hits >= LEAD_LIMIT:
                                logger.info(f"🛑 Lead-Limit erreicht ({LEAD_LIMIT}). Stoppe Scraper …")
                                reached_limit = True
                                break

                    except Exception as e:
                        logger.error(f"❌ Fehler bei Jobkarte {i+1}: {e}")

                if reached_limit:
                    break

            with open(PROGRESS_FILE, 'w') as f:
                f.write(str(idx+1))

            if reached_limit:
                break

        await browser.close()

    # Speichern
    keys = ['keyword','location','title','company','jobs','profile']
    with open(RAW_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(raw_leads)

    unique = {(r['company'], r['profile']): r for r in raw_leads}.values()
    with open(FINAL_CSV, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows(unique)
    logger.info(f"🎉 Fertig: {len(list(unique))} eindeutige Leads gespeichert.")

if __name__ == '__main__':
    try:
        asyncio.run(scrape())
    except KeyboardInterrupt:
        logger.warning("🛑 Abbruch durch Benutzer")
        sys.exit(1)
