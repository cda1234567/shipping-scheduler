FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV APP_PORT=8765

WORKDIR /app

COPY requirements-server.txt /app/requirements-server.txt
RUN pip install --no-cache-dir -r /app/requirements-server.txt

COPY app /app/app
COPY static /app/static
COPY templates /app/templates
COPY config.yaml /app/config.yaml
COPY main.py /app/main.py

VOLUME ["/app/data"]

EXPOSE 8765

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=5 \
  CMD python -c "import sys, urllib.request; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8765/api/health', timeout=3).status == 200 else 1)"

CMD ["python", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8765"]
