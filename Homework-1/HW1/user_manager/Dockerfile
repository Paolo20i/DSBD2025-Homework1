FROM python:3.9-slim

# QUESTA RIGA È FONDAMENTALE PER VEDERE I LOG SUBITO
ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 5000
# Ricorda che la EXPOSE è solo documentale, ma è bene averla corretta
EXPOSE 50051 

CMD ["python", "app.py"]