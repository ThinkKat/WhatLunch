#!/usr/bin/env python3
import os
from datetime import datetime, timedelta, timezone
from typing import List, Tuple, Dict, Any

import boto3
from botocore.exceptions import (
    ClientError,
    NoCredentialsError,
    PartialCredentialsError,
    EndpointConnectionError,
    BotoCoreError,
)
from flask import Flask, request, redirect, url_for, render_template_string, abort

# ===== 설정 =====
BUCKET = os.environ.get("BUCKET", "whatlunch-s3")
SITES = [
    s.strip().lower()
    for s in os.environ.get("SITES", "onbid,autohub,autoinside,automart").split(",")
    if s.strip()
]
AWS_REGION = os.environ.get("AWS_REGION")  # 선택
MAX_BYTES_READ = int(
    os.environ.get("MAX_BYTES_READ", str(1024 * 1024))
)  # CSV 앞부분 읽기 (행수 판정)
CSV_MIN_BYTES = int(os.environ.get("CSV_MIN_BYTES", "200"))
LOG_MIN_BYTES = int(os.environ.get("LOG_MIN_BYTES", "1"))

KST = timezone(timedelta(hours=9))
s3 = boto3.client("s3", region_name=AWS_REGION) if AWS_REGION else boto3.client("s3")

app = Flask(__name__)


def kst_dates(n_days: int) -> List[Tuple[str, str]]:
    """KST 기준 '오늘-1'부터 과거 n일 날짜 목록 (YYYY-MM-DD, YYYYMMDD)"""
    base = datetime.now(KST) - timedelta(days=1)
    out = []
    for i in range(n_days):
        d = base - timedelta(days=i)
        out.append((d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d")))
    return out


def expected_keys(site: str, date_folder: str, date_nodash: str) -> Tuple[str, str]:
    site = site.lower()
    if site == "onbid":
        csv_key = f"raw/onbid/{date_folder}/onbid-{date_nodash}-raw.csv"
        log_key = f"logs/onbid/{date_folder}/crawl_{date_nodash}.log"
    else:
        csv_key = f"raw/{site}/{date_folder}/{site}-{date_nodash}-raw.csv"
        log_key = f"logs/{site}/{date_folder}/crawl_{date_nodash}.log"
    return csv_key, log_key


def head_ok(key: str, min_bytes: int) -> Tuple[bool, int, str]:
    try:
        rsp = s3.head_object(Bucket=BUCKET, Key=key)
        size = rsp.get("ContentLength", 0)
        return (size >= min_bytes, size, "")
    except (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
        EndpointConnectionError,
        BotoCoreError,
    ) as e:
        # 에러 코드를 화면에 노출해 진단 가능하게 함
        err = (
            getattr(e, "response", {})
            .get("Error", {})
            .get("Code", e.__class__.__name__)
        )
        return (False, 0, err)
    except Exception as e:
        return (False, 0, f"Unhandled:{e.__class__.__name__}")


def csv_has_min_lines(key: str, min_lines: int = 2) -> Tuple[bool, int]:
    try:
        rsp = s3.get_object(Bucket=BUCKET, Key=key, Range=f"bytes=0-{MAX_BYTES_READ-1}")
        body = rsp["Body"].read()
        try:
            text = body.decode("utf-8-sig", errors="ignore")
        except Exception:
            text = body.decode("utf-8", errors="ignore")
        lines = [ln for ln in text.strip().splitlines() if ln.strip()]
        return (len(lines) >= min_lines, len(lines))
    except (
        ClientError,
        NoCredentialsError,
        PartialCredentialsError,
        EndpointConnectionError,
        BotoCoreError,
    ):
        return (False, 0)
    except Exception:
        return (False, 0)


def make_console_url(bucket: str, key: str) -> str:
    if not AWS_REGION:
        return ""
    return f"https://s3.console.aws.amazon.com/s3/object/{bucket}?region={AWS_REGION}&prefix={key}"


def check_one(site: str, date_folder: str, date_nodash: str) -> Dict[str, Any]:
    csv_key, log_key = expected_keys(site, date_folder, date_nodash)
    csv_ok, csv_size, csv_err = head_ok(csv_key, CSV_MIN_BYTES)
    log_ok, log_size, log_err = head_ok(log_key, LOG_MIN_BYTES)
    lines_ok, line_count = (False, 0)
    if csv_ok:
        lines_ok, line_count = csv_has_min_lines(csv_key, 2)
    status = csv_ok and lines_ok and log_ok
    miss = (not csv_ok) and (not log_ok)  # 둘 다 없으면 'MISS'
    return {
        "site": site,
        "date_folder": date_folder,
        "csv_key": csv_key,
        "csv_ok": csv_ok,
        "csv_size": csv_size,
        "csv_err": csv_err,
        "csv_lines_ok": lines_ok,
        "csv_line_count": line_count,
        "csv_console": make_console_url(BUCKET, csv_key),
        "log_key": log_key,
        "log_ok": log_ok,
        "log_size": log_size,
        "log_err": log_err,
        "log_console": make_console_url(BUCKET, log_key),
        "ok": status,
        "miss": miss,
    }


@app.get("/")
def root():
    return redirect(url_for("dashboard"))


@app.get("/healthz")
def healthz():
    return {"ok": True, "bucket": BUCKET, "sites": SITES}, 200


@app.get("/dashboard")
def dashboard():
    # 쿼리: date=YYYY-MM-DD (없으면 최근 n일), days=7, sites=comma
    date_q = request.args.get("date")
    days_q = int(request.args.get("days", "7"))
    sites_q = request.args.get("sites")

    sites = [s.strip().lower() for s in sites_q.split(",")] if sites_q else SITES
    sites = [s for s in sites if s]  # 공백 제거

    if date_q:
        try:
            d = datetime.strptime(date_q, "%Y-%m-%d")
        except ValueError:
            abort(400, "date must be YYYY-MM-DD")
        dates = [(d.strftime("%Y-%m-%d"), d.strftime("%Y%m%d"))]
    else:
        dates = kst_dates(days_q)

    # 결과 수집: 날짜별 상세 + 매트릭스용
    results_by_date = []  # 상세 섹션용 [{date, items:[{site,...}]}]
    matrix_dates = [df for (df, dn) in dates]  # 열 헤더에 사용
    matrix_rows = []  # 매트릭스용 [{site, cells:[{status, title, anchor}]}]
    summary = {"total": 0, "pass": 0, "fail": 0, "miss": 0}

    # 날짜 → {site -> result} 맵도 만들어 클릭 시 앵커 연결
    detail_map: Dict[str, Dict[str, Dict[str, Any]]] = {}

    for date_folder, date_nodash in dates:
        row = {"date": date_folder, "items": []}
        detail_map[date_folder] = {}
        for site in sites:
            r = check_one(site, date_folder, date_nodash)
            row["items"].append(r)
            detail_map[date_folder][site] = r
            summary["total"] += 1
            if r["ok"]:
                summary["pass"] += 1
            elif r["miss"]:
                summary["miss"] += 1
            else:
                summary["fail"] += 1
        results_by_date.append(row)

    # 매트릭스 행 구성
    for site in sites:
        cells = []
        for date_folder in matrix_dates:
            r = detail_map[date_folder][site]
            if r["ok"]:
                status = "pass"
            elif r["miss"]:
                status = "miss"
            else:
                status = "fail"
            title = (
                f"{site} @ {date_folder} -> "
                f"CSV:{'OK' if r['csv_ok'] else 'X'}(size={r['csv_size']}, lines={r['csv_line_count']}) "
                f"/ LOG:{'OK' if r['log_ok'] else 'X'}(size={r['log_size']})"
            )
            cells.append(
                {
                    "status": status,
                    "title": title,
                    "anchor": f"#d-{date_folder}",  # 하단 상세 섹션으로 스크롤
                }
            )
        matrix_rows.append({"site": site, "cells": cells})

    return render_template_string(
        TPL,
        bucket=BUCKET,
        region=AWS_REGION,
        sites=sites,
        days=days_q,
        date_q=date_q or "",
        matrix_dates=matrix_dates,
        matrix_rows=matrix_rows,
        results=results_by_date,
        csv_min=CSV_MIN_BYTES,
        log_min=LOG_MIN_BYTES,
        summary=summary,
    )


# ===== HTML 템플릿 (간단 시각화) =====
TPL = r"""
<!doctype html>
<html lang="ko">
<head>
  <meta charset="utf-8">
  <title>Crawler Health Dashboard</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    :root {
      --ok: #16a34a;      /* green-600 */
      --ok-bg: #dcfce7;   /* green-100 */
      --fail: #dc2626;    /* red-600 */
      --fail-bg: #fee2e2; /* red-100 */
      --miss: #6b7280;    /* gray-500 */
      --miss-bg: #f3f4f6; /* gray-100 */
      --card-bg: #f8fafc;
      --border: #e5e7eb;
      --text: #111827;
      --muted: #6b7280;
    }
    body { font-family: system-ui, -apple-system, Segoe UI, Roboto, "Noto Sans KR", Arial, sans-serif; margin: 24px; color: var(--text); }
    h1 { margin: 0 0 12px 0; }
    .meta { color: var(--muted); margin-bottom: 20px; }
    form.filters { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; margin-bottom: 16px; }
    input[type="text"], input[type="date"] { padding: 6px 8px; border: 1px solid var(--border); border-radius: 6px; }
    button { padding: 8px 12px; border: 0; background: #0ea5e9; color: white; border-radius: 8px; cursor: pointer; }
    button:hover { background: #0284c7; }
    .cards { display: flex; gap: 12px; flex-wrap: wrap; margin: 8px 0 16px; }
    .card { background: var(--card-bg); border: 1px solid var(--border); border-radius: 12px; padding: 12px 14px; min-width: 120px; }
    .card h3 { margin: 0 0 6px; font-size: 14px; color: var(--muted); }
    .card .num { font-size: 22px; font-weight: 700; }

    /* 매트릭스(사이트 x 날짜) */
    .matrix-wrap { overflow-x: auto; border: 1px solid var(--border); border-radius: 12px; }
    .matrix { display: grid; grid-auto-rows: 28px; }
    .matrix .hdr { position: sticky; top: 0; background: white; z-index: 1; }
    .matrix .left { position: sticky; left: 0; background: white; z-index: 1; font-weight: 600; padding: 6px 10px; border-right: 1px solid var(--border); }
    .matrix .cell { width: 24px; height: 24px; border-radius: 6px; margin: 2px auto; display: inline-block; border: 1px solid var(--border); }
    .cell.pass { background: var(--ok-bg); border-color: #86efac; }
    .cell.fail { background: var(--fail-bg); border-color: #fca5a5; }
    .cell.miss { background: var(--miss-bg); border-color: #d1d5db; }
    .legend { display: flex; align-items: center; gap: 10px; color: var(--muted); margin-top: 8px; }
    .legend .dot { width: 12px; height: 12px; border-radius: 4px; display: inline-block; border: 1px solid var(--border); margin-right: 4px; }
    .legend .pass { background: var(--ok-bg); border-color: #86efac; }
    .legend .fail { background: var(--fail-bg); border-color: #fca5a5; }
    .legend .miss { background: var(--miss-bg); border-color: #d1d5db; }

    table { border-collapse: collapse; width: 100%; margin-top: 16px; }
    th, td { border: 1px solid var(--border); padding: 8px 10px; font-size: 14px; vertical-align: top; }
    th { background: #f8fafc; text-align: left; }
    .mono { font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, "Liberation Mono", monospace; }
    .sub { color: var(--muted); font-size: 12px; }
    .badge { padding: 2px 6px; border-radius: 6px; font-size: 12px; }
    .b-ok { background: var(--ok-bg); color: var(--ok); }
    .b-fail { background: var(--fail-bg); color: var(--fail); }
  </style>
</head>
<body>
  <h1>Crawler Health Dashboard</h1>
  <div class="meta">Bucket: <b>{{ bucket }}</b>{% if region %} (region: {{ region }}){% endif %}</div>

  <form class="filters" method="get" action="{{ url_for('dashboard') }}">
    <label>날짜(없으면 최근 {{days}}일): <input type="date" name="date" value="{{ date_q }}"></label>
    <label>일수: <input type="text" name="days" value="{{days}}" size="3"></label>
    <label>사이트(콤마): <input type="text" name="sites" value="{{ ','.join(sites) }}" size="32"></label>
    <button type="submit">조회</button>
  </form>

  <!-- 요약 카드 -->
  <div class="cards">
    <div class="card"><h3>Total</h3><div class="num">{{ summary.total }}</div></div>
    <div class="card"><h3>PASS</h3><div class="num">{{ summary.pass }}</div></div>
    <div class="card"><h3>FAIL</h3><div class="num">{{ summary.fail }}</div></div>
    <div class="card"><h3>MISS</h3><div class="num">{{ summary.miss }}</div></div>
  </div>

  <!-- 매트릭스(간단 히트맵) -->
  <div class="matrix-wrap">
    {% set cols = matrix_dates|length + 1 %}
    <div class="matrix" style="grid-template-columns: 160px repeat({{ matrix_dates|length }}, 32px);">
      <!-- 헤더: 빈칸 + 날짜들 -->
      <div class="left hdr">Site \ Date</div>
      {% for d in matrix_dates %}
        <div class="hdr" style="display:flex;align-items:center;justify-content:center;font-size:12px;color:#555;">{{ d[5:] }}</div>
      {% endfor %}

      <!-- 각 사이트 행 -->
      {% for row in matrix_rows %}
        <div class="left">{{ row.site }}</div>
        {% for c in row.cells %}
          <a href="{{ c.anchor }}" title="{{ c.title }}"><div class="cell {{ c.status }}"></div></a>
        {% endfor %}
      {% endfor %}
    </div>
  </div>
  <div class="legend">
    <span><span class="dot pass"></span>PASS</span>
    <span><span class="dot fail"></span>FAIL</span>
    <span><span class="dot miss"></span>MISS</span>
  </div>

  <!-- 날짜별 상세 -->
  {% for block in results %}
    <h2 id="d-{{ block.date }}" style="margin-top:24px;">{{ block.date }}</h2>
    <table>
      <thead>
        <tr>
          <th style="width:140px;">Site</th>
          <th>CSV</th>
          <th>Log</th>
          <th>Status</th>
        </tr>
      </thead>
      <tbody>
        {% for item in block["items"] %}
        <tr>
          <td><b>{{ item.site }}</b></td>
          <td>
            <div class="mono">s3://{{ bucket }}/{{ item.csv_key }}</div>
            <div class="sub">size: {{ item.csv_size }} bytes (min {{ csv_min }}), lines ≥2? {{ 'YES' if item.csv_lines_ok else 'NO' }} ({{ item.csv_line_count }})</div>
            {% if item.csv_console %}
              <div class="sub"><a href="{{ item.csv_console }}" target="_blank" rel="noopener">Open in AWS Console</a></div>
            {% endif %}
            {% if item.csv_err and not item.csv_ok %}
              <div class="sub">error: {{ item.csv_err }}</div>
            {% endif %}
          </td>
          <td>
            <div class="mono">s3://{{ bucket }}/{{ item.log_key }}</div>
            <div class="sub">size: {{ item.log_size }} bytes (min {{ log_min }})</div>
            {% if item.log_console %}
              <div class="sub"><a href="{{ item.log_console }}" target="_blank" rel="noopener">Open in AWS Console</a></div>
            {% endif %}
            {% if item.log_err and not item.log_ok %}
              <div class="sub">error: {{ item.log_err }}</div>
            {% endif %}
          </td>
          <td>
            {% if item.ok %}
              <span class="badge b-ok">PASS</span>
            {% else %}
              <span class="badge b-fail">FAIL</span>
            {% endif %}
          </td>
        </tr>
        {% endfor %}
      </tbody>
    </table>
  {% endfor %}
</body>
</html>
"""

if __name__ == "__main__":
    port = int(os.environ.get("PORT", "8080"))
    app.run(host="0.0.0.0", port=port)
