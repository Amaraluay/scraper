import os
BOT_NAME = "stepstone_leads"
SPIDER_MODULES = ["stepstone_leads.spiders"]
NEWSPIDER_MODULE = "stepstone_leads.spiders"

# Playwright
DOWNLOAD_HANDLERS = {"http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
                     "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler"}
TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"
PLAYWRIGHT_BROWSER_TYPE = "chromium"
PLAYWRIGHT_LAUNCH_OPTIONS = {
    "headless": True,
    "args": ["--no-sandbox", "--disable-http2"]
}

# Concurrency / Robustness
CONCURRENT_REQUESTS = int(os.getenv("CONCURRENT_REQUESTS", "8"))
PLAYWRIGHT_DEFAULT_NAVIGATION_TIMEOUT = 30_000
DOWNLOAD_TIMEOUT = 45
RETRY_ENABLED = True
RETRY_TIMES = 3
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 0.5
AUTOTHROTTLE_MAX_DELAY = 5

# Proxy (optional)
proxy_url = os.getenv("PROXY_URL")  # z.B. http://user:pass@host:port
if proxy_url:
    DOWNLOADER_MIDDLEWARES = {
        "scrapy.downloadermiddlewares.httpproxy.HttpProxyMiddleware": 750,
        "scrapy.downloadermiddlewares.retry.RetryMiddleware": 550,
    }
    HTTPPROXY_ENABLED = True
    HTTPPROXY_AUTH_ENCODING = "utf-8"

# FEEDS (CSV mit Timestamp)
from datetime import datetime
ts = datetime.now().strftime("%Y%m%d_%H%M%S")
FEEDS = {
    f"stepstone_leads_{ts}.csv": {"format": "csv", "overwrite": True},
}
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Jobdir für Persistenz/Resume
JOBDIR = os.getenv("JOBDIR", ".jobdir")
