# Scrapers

- daily
- prev

docker build -t onbid-scraper

docker run --rm -it \
  --shm-size=1g \
  -v "$PWD/base:/app/base" \
  -v "$PWD/result:/app/result" \
  onbid-scraper
