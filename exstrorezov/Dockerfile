FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY *.py .

# Render монтирует постоянный диск в /app/data

CMD ["python", "bot_mtproto.py"]
