FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

# Для данных
VOLUME ["/app/data"]

# Render даёт PORT через переменную окружения
ENV HOST=0.0.0.0
EXPOSE 8080

CMD ["python", "bot_mtproto.py"]
