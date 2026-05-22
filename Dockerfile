FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY big_holder_web.py .

# Expose port (Railway/Render override with PORT env var)
EXPOSE 8001

CMD ["python", "big_holder_web.py"]
