FROM mcr.microsoft.com/playwright:v1.50.0-jammy
WORKDIR /app

# Update and install system dependencies
RUN apt-get update && apt-get install -y \
    postgresql-client \
    libpq-dev \
    gcc \
    python3 \
    python3-pip \
    python3-dev

# Ensure pip is up to date
RUN python3 -m pip install --upgrade pip

# Copy requirements file
COPY requirements.txt .

# Install Python dependencies
RUN python3 -m pip install -r requirements.txt

# Copy application code
COPY . .

# Ensure Playwright browsers are installed correctly
RUN playwright install chromium

# Environment variables
ENV PLAYWRIGHT_BROWSERS_PATH=/ms-playwright
ENV PYTHONUNBUFFERED=1
ENV PATH="/usr/bin:${PATH}"

# Verify Python installation
RUN python3 --version && pip3 --version

# Default command
CMD ["python3", "main.py"]
