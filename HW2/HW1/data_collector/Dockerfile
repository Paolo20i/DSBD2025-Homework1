FROM python:3.9-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

# Espone una porta diversa per evitare conflitti
EXPOSE 5001

CMD ["python", "app.py"]