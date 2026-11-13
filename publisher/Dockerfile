FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /srv/publisher
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY publisher.py .

# Keep the container alive; run scenarios with `docker compose run`.
CMD ["sleep", "infinity"]
