FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends ffmpeg \
    && rm -rf /var/lib/apt/lists/*
RUN pip install --no-cache-dir onvif-zeep-async==3.2.5 ffmpeg-python aiohttp
COPY onvifeye.py /app/onvifeye.py
CMD ["python", "-u", "/app/onvifeye.py"]
