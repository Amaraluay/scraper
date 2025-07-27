FROM python:3.11

# System-Updates und notwendige Tools
RUN apt-get update && apt-get install -y curl unzip wget gnupg libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 libxshmfence1 libgbm-dev libxrandr2 xvfb

# Arbeitsverzeichnis
WORKDIR /app

# Projektdateien kopieren
COPY . .

# Abhängigkeiten
RUN pip install --upgrade pip && pip install -r requirements.txt

# Playwright installieren + Browser
RUN pip install playwright && playwright install --with-deps

CMD ["python", "scraper_github.py"]
