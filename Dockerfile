FROM mcr.microsoft.com/playwright:v1.50.0-jammy

WORKDIR /app

# Copy requirements first
COPY requirements.txt .

# Install PostgreSQL dependencies
RUN apt-get update && apt-get install -y postgresql-client libpq-dev gcc python3-dev

# Install Python dependencies using python -m pip instead of just pip
RUN python -m pip install -r requirements.txt

# Copy application code
COPY . .

# Ensure Playwright browsers are installed correctly
RUN playwright install chromium

# Environment variables for Playwright
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PYTHONUNBUFFERED=1

# Default command (will be overridden by Railway)
CMD ["python3", "backend.py"]
