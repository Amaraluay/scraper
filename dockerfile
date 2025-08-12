# Robustes Base-Image mit allen Playwright/Chromium-Deps
FROM mcr.microsoft.com/playwright/python:v1.45.0-jammy

# App rein
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Dein Skript/Repo
COPY . .

# Logs & CSVs sollen auf die persistente Disk -> HOME=/data, CWD=/data
WORKDIR /data
ENV HOME=/data
ENV PYTHONUNBUFFERED=1
ENV TZ=Europe/Berlin

# Optional: Browser schon im Build ziehen (reduziert Cold-Starts)
RUN playwright install --with-deps chromium

# Start
CMD ["python", "/app/stepstone_scraper.py"]
