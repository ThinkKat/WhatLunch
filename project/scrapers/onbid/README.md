# ONBID 중고차 경매 크롤러

ONBID 자동차 경매의 종료된 입찰 결과(HISTORY)와 진행/예정 공고(NEW)를 수집해 CSV로 저장하는 프로젝트입니다. Docker 컨테이너로 하루치/한 달치 배치를 쉽게 실행할 수 있습니다.

---

## 주요 기능

* `crawl_base.py`: 기간, 분류(승용차/SUV/승합차)를 기준으로 수집할 목록 설정
    - input: 모드, 경매 시작/종료 날짜, 분류, 저장경로
    - output: 물건의 대략적인 정보가 포함된 JSON파일

* `crawl_detail.py`: `crawl_base.py`에서 수집한 목록을 대상으로 물건관리번호로 상세 페이지 진입 후 기본 정보를 수집
    - input: 모드, crawl_base의 output, 워커 수, 저장 경로
    - output: 물건의 자세한 정보가 포함된 CSV파일

* 모드별 동작
  * **HISTORY**: 개찰 완료된 경매 결과.
  * **NEW**: 진행 중/예정 경매 공고.

* 배치 스크립트
  * `daily_crawler.sh`: 어제 날짜 1일치 → JSON(베이스) + CSV(상세).
  * `monthly_crawler.sh`: 오늘부터 +30일 범위(NEW) → JSON + CSV.

---

## 디렉토리 구조

```
project-root
 ┣ base
 | ┗ base_info.json
 ┣ result
 | ┗ detail_info.csv
 ┣ 📄 crawl_base.py          # 목록(베이스) 수집
 ┣ 📄 crawl_detail.py        # 상세 수집(Playwright)
 ┣ 📄 daily_crawler.sh       # 어제 1일치 배치(HISTORY)
 ┣ 📄 monthly_crawler.sh     # +30일 배치(NEW)
 ┣ 📄 Dockerfile
 ┗ 📄 requirements.txt
```

---

## 설치 & 빌드

### 1) Docker 이미지 빌드

```bash
docker build -t onbid-scraper .
```

### 2) 컨테이너 실행 (기본: 하루치 HISTORY 배치)

```bash
# 결과는 /app/base, /app/result 아래 생성
docker run --rm -v "$PWD:/app" --shm-size=1g onbid-scraper
```

> `CMD ["/app/daily_crawler.sh"]`가 기본값이므로 실행만으로 어제 데이터가 수집됩니다.

---

## 스크립트로 실행

### 1) 하루치(HISTORY)

```bash
docker run -it --rm -v "$PWD:/app" --shm-size=1g onbid-scraper /bin/bash
bash daily_crawler.sh
```

* 출력 예시
  * 베이스 JSON: `./base/2025-08-22.json`
  * 상세 CSV: `./result/2025-08-22.csv`

### 2) 30일 범위(NEW)

```bash
docker run -it --rm -v "$PWD:/app" --shm-size=1g onbid-scraper /bin/bash
bash monthly_crawler.sh
```

* 출력 예시
  * 베이스 JSON: `./base/new_YYYY-MM-DD_YYYY-MM-DD.json`
  * 상세 CSV: `./result/new_YYYY-MM-DD_YYYY-MM-DD.csv`

---

## 개별 크롤러 설명

### 1) 목록(베이스) 수집: `crawl_base.py`

```bash
# 종료된 경매 검색(HISTORY)
python crawl_base.py \
  --mode HISTORY \
  --from 2024-08-02 --to 2025-08-01 \
  --categories 12101 12102 12103 \
  --out base/history_base.json \
  --max-pages 500

# 진행/예정 경매 검색(NEW)
python crawl_base.py \
  --mode NEW \
  --from 2025-08-01 --to 2025-08-28 \
  --categories 12101 12102 12103 \
  --out base/new_base.json \
  --max-pages 50
```

**주요 옵션**

* `--mode`: `NEW | HISTORY`
* `--from`, `--to`: `YYYY-MM-DD` (최대 1년 범위)
* `--categories`: 소분류 코드(기본: 12101 승용차, 12102 SUV, 12103 승합차)
* `--max-pages`: 각 카테고리별 최대 페이지 수집 한도
* `--out`: 베이스 JSON 저장 경로

### 2) 상세 수집: `crawl_detail.py`

물건관리번호로 검색 후, 결과의 첫 링크에서 `fn_selectDetail(...)` 파라미터를 추출해 상세 페이지로 진입, **물건정보** 탭의 테이블을 병합해 CSV로 저장합니다.

```bash
# HISTORY 상세 수집
python crawl_detail.py \
  --mode HISTORY \
  --input base/history_base.json \
  --out result/history_detail.csv \
  --workers 6

# NEW 상세 수집
python crawl_detail.py \
  --mode NEW \
  --input base/new_base.json \
  --out result/new_detail.csv \
  --workers 6 --headful
```

**주요 옵션**

* `--input`: 베이스 JSON 경로 (`crawl_base.py` 출력)
* `--mode`: `NEW | HISTORY`
* `--workers`: 병렬 처리 프로세스 수 (과도하면 차단 위험)
* `--headful`: 브라우저 창 표시 (디버깅에 유용)
* `--timeout`: 단계별 타임아웃(ms), 기본 `200000`
* `--out`: CSV 저장 경로

---

## 크론 예시

하루 한 번 01:30에 실행(어제 데이터 수집):

```cron
30 1 * * * docker run --rm -v /data/onbid:/app --shm-size=1g onbid-scraper >> /data/onbid/onbid_cron.log 2>&1
```

---

## 트러블슈팅

* **Timeout while processing ...**

  * `--timeout` 상향, `--workers` 축소, 네트워크 확인.