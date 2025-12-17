# Dockerfile
FROM python:3.9-slim
WORKDIR /app
RUN pip install --no-cache-dir requests

COPY iot_emulator.py .
COPY cfg.json .

CMD ["python", "iot_emulator.py"]
