FROM python:3.11-slim

WORKDIR /app

# System deps for opencv-python-headless (required by ddddocr)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app files
COPY big_holder_web.py .
COPY twse_bshtm_crawler.py .

# Expose port (Railway/Render override with PORT env var)
EXPOSE 8001

CMD ["python", "big_holder_web.py"]
