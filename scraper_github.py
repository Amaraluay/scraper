async def apply_stealth(page):
    await page.add_init_script("""
        Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
        window.chrome = { runtime: {} };
        Object.defineProperty(navigator, 'languages', { get: () => ['de-DE', 'de'] });
        Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3, 4, 5] });
    """)

# -----------------------------
# Main scraping
# -----------------------------
async def scrape():
    raw_leads: List[Dict] = []
    seen_companies: Set[str] = set()
    access_denied_count = 0

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=False)
        context = await browser.new_context(
            proxy={
                "server": PROXY_SERVER,
                "username": PROXY_USER,
                "password": PROXY_PASS
            }
        )
        page = await context.new_page()
        await apply_stealth(page)
# -----------------------------
# Suchparameter-Generator
# -----------------------------
KEYWORDS = [
    "gesundheits-und-krankenpfleger",
    "pflegehilfskraft",
    "servicetechniker",
    "aussendienst",
    "produktionsmitarbeiter",
    "maschinen-und-anlagenfuehrer",
    "fertigungsmitarbeiter",
    "kundenberater",
    "kundenservice",
    "kundendienstberater"
]

LOCATIONS_WITH_RADIUS = [
    ("regensburg", 50),
    ("würzburg", 50),
    ("freiburg", 50),
    ("ingolstadt", 50),
    ("trier", 50),
    ("saarbrücken", 50),
    ("konstanz", 30),
    ("nürnberg", 50),
    ("passau", 50)
]

# Automatisch kombinieren
SEARCH_PARAMS = [
    (keyword, location, radius)
    for keyword in KEYWORDS
    for location, radius in LOCATIONS_WITH_RADIUS
]

        for keyword, location, radius in SEARCH_PARAMS:
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
                    logger.warning(f"🚫 Access denied (#{access_denied_count}) auf {url}")
                    if access_denied_count >= MAX_DENIED:
                        logger.warning("💤 Zu viele Access Denied – Zwischenspeichern & 5 Minuten warten")
                        break
                    await asyncio.sleep(5)
                    continue

                cards = page.locator("article[data-at='job-item']")
                count = await cards.count()
                logger.info(f"📦 Gefundene Jobkarten: {count}")
                if count == 0:
                    break

                for i in range(count):
                    card = cards.nth(i)
                    try:
                        title = await card.locator("[data-testid='job-item-title'] div.res-ewgtgq").inner_text()
                        company = await card.locator("span[data-at='job-item-company-name'] span.res-du9bhi").inner_text()
                    except:
                        continue
                    if company in seen_companies:
                        continue

                    href = await card.locator("a[data-at='company-logo']").get_attribute('href')
                    if not href:
                        continue
                    profile_url = href if href.startswith('http') else f"https://www.stepstone.de{href}"

                    prof_page = await context.new_page()
                    await apply_stealth(prof_page)
                    try:
                        await prof_page.goto(profile_url, wait_until="domcontentloaded", timeout=30000)
                        try:
                            await accept_all_cookies(prof_page)
                            job_count = await get_job_count(prof_page)
                            logger.info(f"🔎 {company}: {job_count} Jobs")

                            if job_count == 0:
                                logger.info(f"🔁 Fallback-Suche für {company}")
                                fallback_search_url = f"https://www.stepstone.de/jobs/in-deutschland?keywords={company.replace(' ', '%20')}"
                                try:
                                    await prof_page.goto(fallback_search_url, wait_until="domcontentloaded", timeout=30000)
                                    await accept_all_cookies(prof_page)
                                    job_count = await get_job_count(prof_page)
                                    logger.info(f"🔎 Fallback-Ergebnis für {company}: {job_count} Jobs")
                                except Exception as e:
                                    logger.error(f"❌ Fehler bei Fallback-Suche für {company}: {e}")
                        except Exception as e:
                            logger.error(f"❌ Fehler beim Profilabruf für {company}: {e}")

                        logger.info(f"🔎 {company}: {job_count} Jobs")
                    except Exception as e:
                        logger.error(f"❌ Fehler bei {profile_url}: {e}")
                        await prof_page.close()
                        continue
                    await prof_page.close()

                    if MIN_JOBS <= job_count <= MAX_JOBS:
                        seen_companies.add(company)
                        entry = {
                            'keyword': keyword,
                            'location': location,
                            'title': title.strip(),
                            'company': company.strip(),
                            'jobs': job_count,
                            'profile': profile_url
                        }
                        raw_leads.append(entry)

        await browser.close()

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
