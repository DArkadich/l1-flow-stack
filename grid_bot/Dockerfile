FROM python:3.11-slim

WORKDIR /app

# Установка зависимостей
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копирование кода
COPY . .

# Создание общей директории
RUN mkdir -p /app/shared

# Запуск бота
CMD ["python", "main.py"]
