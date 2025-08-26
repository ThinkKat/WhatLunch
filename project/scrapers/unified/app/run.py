import os
import subprocess
import sys


def main():
    crawler_name = os.environ.get("CRAWLER_NAME")
    if not crawler_name:
        print("[ERROR] CRAWLER_NAME environment variable not set.", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Running crawler: {crawler_name}")

    if crawler_name == "autoinside":
        subprocess.run(["python", "/app/sites/autoinside_daily_ec2.py"], check=True)
    elif crawler_name == "autohub":
        subprocess.run(["python", "/app/sites/autohub_daily_ec2.py"], check=True)
    elif crawler_name == "onbid_daily":
        subprocess.run(["/app/daily_crawler.sh"], check=True)
    elif crawler_name == "automart":
        # 새로 추가된 automart 크롤러 실행
        subprocess.run(["python", "/app/sites/automart_daily_ec2.py"], check=True)
    else:
        print(f"[ERROR] Unknown crawler name: {crawler_name}", file=sys.stderr)
        sys.exit(1)

    print(f"[INFO] Crawler finished: {crawler_name}")


if __name__ == "__main__":
    main()
