# Scraper - Onbid

docker build -t onbid-scraper .

docker run --rm -it \          
  -v "$PWD:/app" \
  --shm-size=1g \
  onbid-scraper

docker run --rm -it \
  -v "$PWD:/app" \
  --shm-size=1g \   
  onbid-scraper /app/monthly_crawler.sh