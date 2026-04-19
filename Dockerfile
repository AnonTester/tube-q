# -------- Base image with ffmpeg ----------
FROM ubuntu:22.04 AS base

ENV DEBIAN_FRONTEND=noninteractive

# Install Python, basic tools, yt-dlp and other dependencies
RUN apt-get update && \
    apt-get install -y python3 python3-pip \
      curl wget gnupg ca-certificates jq ffmpeg && \
    rm -rf /var/lib/apt/lists/* && \
    pip3 install --no-cache-dir yt-dlp fastapi uvicorn jinja2 sse-starlette aiohttp requests

WORKDIR /app

# Copy service code
COPY tube-q.py /app/
COPY favicon.ico /app/
COPY apple-touch-icon.png /app/
COPY logo.png /app/

# Default config folder in container (bind-mounted from host)
VOLUME ["/app/conf"]

# Downloads directory (bind-mounted from host)
VOLUME ["/downloads"]

# Optional binaries override (bind-mounted from host)
VOLUME ["/binaries"]

# Default ENV paths (will be overridden if /binaries is mounted)
ENV PATH="/binaries:${PATH}"
ENV YT_DLP_BINARY="/binaries/yt-dlp"
ENV FFMPEG_BINARY="/binaries/ffmpeg"
ENV FFPROBE_BINARY="/binaries/ffprobe"

EXPOSE 7090

CMD ["python3", "/app/tube-q.py"]
