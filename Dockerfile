FROM python:3.13-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY vanvalor-bot.py .
COPY cogs/ cogs/

VOLUME /app/data

CMD ["python", "vanvalor-bot.py"]
