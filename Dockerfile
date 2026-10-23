FROM python:3.12-slim

# ffmpeg + libopus: bắt buộc để phát âm thanh trong kênh thoại
RUN apt-get update \
    && apt-get install -y --no-install-recommends ffmpeg libopus0 \
    && rm -rf /var/lib/apt/lists/*

# deno: yt-dlp cần một JavaScript runtime để lấy được link YouTube
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY music_bot.py .

ENV PYTHONUNBUFFERED=1
CMD ["python", "music_bot.py"]
