#!/usr/bin/env python3
import os, sys, subprocess

# setproctitle은 선택 의존성입니다. 없으면 no-op으로 대체합니다.
try:
    from setproctitle import setproctitle
except Exception:

    def setproctitle(_):
        pass


CRAWLER_ID = os.getenv("CRAWLER_ID", "").strip()
CRAWLER_NAME = os.getenv("CRAWLER_NAME", CRAWLER_ID).strip() or "crawler"
DATE = os.getenv("DATE")

ENTRYPOINTS = {
    "autoinside": ["python", "-u", "sites/autoinside_daily_ec2.py"],
    "autohub": ["python", "-u", "sites/autohub_daily_ec2.py"],
    "onbid_daily": ["/bin/bash", "-lc", "/app/daily_crawler.sh"],
}


def main():
    if CRAWLER_ID not in ENTRYPOINTS:
        print(f"[ERR] Unknown CRAWLER_ID={CRAWLER_ID}", file=sys.stderr)
        sys.exit(2)

    # ps/top에서 식별 가능하도록 프로세스명 지정
    setproctitle(f"crawler:{CRAWLER_NAME}")

    env = os.environ.copy()
    if DATE:
        env["DATE"] = DATE

    cmd = ENTRYPOINTS[CRAWLER_ID]
    print(f"[INFO] Launch: {CRAWLER_NAME} -> {' '.join(cmd)}")
    proc = subprocess.run(cmd, env=env)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
