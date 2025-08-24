FROM python:3.11-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends tzdata build-essential && rm -rf /var/lib/apt/lists/*
COPY l1_bot/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY l1_bot/main.py .
CMD ["python", "-u", "main.py"]
