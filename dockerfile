# Use official Python base
FROM python:3.11-slim

# Set workdir
WORKDIR /app

# Install system dependencies for Playwright + Chromium
RUN apt-get update && apt-get install -y \
    wget curl unzip fonts-liberation libappindicator3-1 libasound2 \
    libatk-bridge2.0-0 libatk1.0-0 libcups2 libdbus-1-3 libgdk-pixbuf2.0-0 \
    libnspr4 libnss3 libxcomposite1 libxdamage1 libxrandr2 xdg-utils \
    libu2f-udev libvulkan1 xvfb && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Python requirements
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Install Playwright + browsers
RUN playwright install chromium

# Copy source code
COPY . .

# Run script
CMD ["python", "scraper_github.py"]
