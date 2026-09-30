# Image receiving HL7 messages from Epic over TCP (MLLP) on port 20480, with
# a HTTP status check on port 8000
#
# See docker-compose.yml for how to build, copy and run the image on the
# server with restricted internet access

FROM python:3.12-slim

# don't write .pyc files and don't buffer the output
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    RESPONSE_DIR=/app/responses \
    LOG_FILE=/app/logs/hl7_receiving_messages.log

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# only copy the code needed for receiving messages
COPY hl7/ hl7/
COPY hl7_receiving.py .

# run as a non root user. UID 1000 so that files written to mounted host
# directories belong to the default host user
RUN useradd --uid 1000 --no-create-home appuser \
    && mkdir -p /app/responses /app/logs \
    && chown appuser /app/responses /app/logs
USER appuser

# HL7 messages (TCP) and status check (HTTP)
EXPOSE 20480 8000

# mark the container as unhealthy if the status check stops responding
HEALTHCHECK --interval=30s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/')"

# a single worker as only one process can listen on the TCP port
CMD ["uvicorn", "hl7_receiving:app", "--host", "0.0.0.0", "--port", "8000"]
