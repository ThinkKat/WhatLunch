# 통합 Scraper

        /opt/crawler
        ├─ compose.yaml # 사이트별 서비스(컨테이너) – 단일 이미지 공통 사용
        ├─ crawl_plan.sh # 직렬 실행(순차) 배치 스크립트
        ├─ register_cron.sh # 크론 등록 스크립트 (매일 02:00)
        ├─ unregister_cron.sh # 크론 해제 스크립트
        ├─ app/
        │ ├─ Dockerfile # Playwright 베이스 이미지 사용
        │ ├─ requirements.txt # (playwright 제외) 크롤링 라이브러리
        │ ├─ run.py # CRAWLER_ID/NAME로 엔트리포인트 스위칭
        │ ├─ daily_crawler.sh # 온비드 베이스→디테일 일괄 실행(어제자)
        │ ├─ sites/
        │ │ ├─ autoinside_daily_ec2.py # AutoInside 어제치 수집(Chromium)
        │ │ └─ autohub_daily_ec2.py # AutoHub 어제치 수집(Chromium)
        │ └─ onbid/
        │ ├─ crawl_base.py # 온비드 목록 수집(requests/bs4)
        │ └─ crawl_detail.py # 온비드 상세 수집(Firefox)
        └─ data/ # (선택) 호스트 마운트 경로
        ├─ onbid/base/
        └─ onbid/result/