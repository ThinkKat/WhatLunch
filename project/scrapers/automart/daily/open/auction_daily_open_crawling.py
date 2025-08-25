#!/usr/bin/env python3
# orchestrator.py
# 사용법: python orchestrator.py

import json
import sys
import time
import subprocess
from pathlib import Path
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--url", required=True)
parser.add_argument("--date", required=False)
# 하이픈/언더스코어 둘 다 받기
parser.add_argument("--timeout-sec", "--timeout_sec",
                    dest="timeout_sec", type=int, default=300)

args = parser.parse_args()
URL = args.url
DATE = args.date
TIMEOUT_SEC = args.timeout_sec
# === 사용자 입력 변수 ===
print(URL, DATE, TIMEOUT_SEC)

# 파일/스크립트 경로
BASE_DIR = Path("./automart/daily/closed").resolve()
URL_JSON = BASE_DIR / "auction_url_check_list.json"
COMPLETE_JSON = BASE_DIR / f"complete_data_{DATE}.json"

URL_SCRIPTS = BASE_DIR / "auction_daily_closed_crawling_auction_url_list.py"
CAR_INFO_SCRIPT = BASE_DIR / "auction_daily_closed_crawling_car_info.py"

# === 설정 ===
SLEEP_SEC_AFTER_CAR_INFO = 10       # car_info 스크립트 1회 실행 후 잠깐 쉼
PRINT_PROGRESS_EVERY_SEC = 10      # 진행 상황 주기 출력(초)
MAX_EMPTY_RETRY = 5                # json이 비어있을 때 재시도 횟수


def choose_existing(*paths: Path) -> Path | None:
    for p in paths:
        if p.exists():
            return p
    return None

def run_subprocess(py_file: Path, timeout_sec: int | None = None, *args: str) -> int:
    """
    하위 스크립트 실행. (args는 선택)
    스크립트가 인자를 안 받더라도 에러 없이 동작하도록
    1) 인자 붙여 실행해보고,
    2) 실패하면 인자 없이 재시도.
    """
    cmd_with_args = [sys.executable, str(py_file), *args]
    cmd_no_args = [sys.executable, str(py_file)]

    try:
        return subprocess.run(cmd_with_args, check=True, timeout = timeout_sec).returncode
    except subprocess.CalledProcessError:
        # 인자를 안 받는 스크립트일 수도 있으니 인자 없이 재시도
        return subprocess.run(cmd_no_args, check=True, timeout = timeout_sec).returncode
    except subprocess.TimeoutExpired:
        return -9


def load_check_map(path: Path) -> dict[str, int]:
    with path.open("r", encoding="utf-8") as f:
        data = json.load(f)
    # 값이 0/1이 아닌 True/False 등으로 들어와도 안정적으로 처리
    norm = {}
    for k, v in data.items():
        try:
            norm[k] = int(v)
        except Exception:
            norm[k] = 1 if v else 0
    return norm


def all_done(check_map: dict[str, int]) -> bool:
    return all(v == 1 for v in check_map.values()) if check_map else False


def summary(check_map: dict[str, int]) -> tuple[int, int, int]:
    total = len(check_map)
    done = sum(1 for v in check_map.values() if v == 1)
    pending = total - done
    return total, done, pending


def main():
    # 1) URL 리스트 생성 스크립트 선택 & 실행
    while not URL_JSON.exists():
        print(URL_SCRIPTS, URL_JSON)
        print(f"[STEP 1] URL 리스트 생성: {URL_SCRIPTS}")
        # URL/DATE를 인자로 넘겨보고, 실패 시 인자 없이 재시도
        while True:
            try:
                returncode = run_subprocess(URL_SCRIPTS, TIMEOUT_SEC, "--url", URL, "--date", DATE)
            except Exception as e:
                print(f"[WARN] 인자 전달 실행 실패, 인자 없이 재시도: {e}")
                returncode = run_subprocess(URL_SCRIPTS, TIMEOUT_SEC)
            
            if returncode != -9:
                break

    # URL JSON 확인(없으면 대기/재시도)
    empty_retry = 0
    while not URL_JSON.exists():
        if empty_retry >= MAX_EMPTY_RETRY:
            print(f"[ERR] {URL_JSON} 생성 안 됨. 스크립트/경로 확인 필요.")
            sys.exit(2)
        empty_retry += 1
        print(f"[WAIT] {URL_JSON} 생성 대기 중... ({empty_retry}/{MAX_EMPTY_RETRY})")
        time.sleep(2)

    # 2) check json을 기반으로 반복 처리
    print(f"[STEP 2] 진행 시작: {URL_JSON} 감시 → 모두 1이 되면 종료")
    last_print = 0.0
    loop_count = 0

    while True:
        try:
            check_map = load_check_map(URL_JSON)
        except Exception as e:
            print(f"[WARN] JSON 로드 실패({e}), 잠시 후 재시도")
            time.sleep(2)
            continue

        total, done, pending = summary(check_map)
        now = time.time()
        if now - last_print >= PRINT_PROGRESS_EVERY_SEC:
            print(f"[PROGRESS] 전체 {total}개 | 완료 {done} | 대기 {pending}")
            last_print = now

        if all_done(check_map):
            print("[DONE] 모든 URL 처리 완료! 종료.")
            break
        
        # 3) 아직 남았으면 car_info 스크립트 1회 실행
        if not CAR_INFO_SCRIPT.exists():
            print(f"[ERR] {CAR_INFO_SCRIPT} 를 찾을 수 없음.")
            sys.exit(3)

        loop_count += 1
        print(f"[RUN] car info #{loop_count}: {CAR_INFO_SCRIPT}")
        
        while True:
            try:
                # DATE를 인자로 넘겨보고, 실패 시 인자 없이 실행
                returncode = run_subprocess(CAR_INFO_SCRIPT, TIMEOUT_SEC, "--date", DATE)
            except Exception as e:
                print(f"[WARN] 인자 전달 실패, 인자 없이 재시도: {e}")
                returncode = run_subprocess(CAR_INFO_SCRIPT, TIMEOUT_SEC)

            if returncode != -9:
                break

        if total == 0:
            print("[WARN] URL 목록이 비어 있음.")
            break

        # 파일이 누적되는지(선택) 간단 체크
        if COMPLETE_JSON.exists():
            try:
                # 크기만 빠르게 체크
                size_kb = COMPLETE_JSON.stat().st_size / 1024.0
                print(f"[INFO] {COMPLETE_JSON} 크기 ≈ {size_kb:.1f} KB")
            except Exception:
                pass

        time.sleep(SLEEP_SEC_AFTER_CAR_INFO)

    print("[EXIT] 작업을 정상 종료합니다.")

if __name__ == "__main__":
    main()
