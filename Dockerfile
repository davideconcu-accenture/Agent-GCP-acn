# Usa Python 3.11 slim
FROM python:3.11-slim

# Set working directory
WORKDIR /app

# Installa dipendenze di sistema (per pandas, openpyxl)
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copia SOLO requirements prima (per cache Docker)
COPY requirements.txt .

# Installa dipendenze Python
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Copia il resto del codice
COPY . .

# Installa il package in modalità development
RUN pip install --no-cache-dir -e .

# Esponi porta 8080
EXPOSE 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD python -c "import requests; requests.get('http://localhost:8080/health')" || exit 1

# Comando di avvio con gunicorn
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 120 app:app
