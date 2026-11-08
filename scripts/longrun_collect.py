#!/usr/bin/env python3
"""
longrun_collect.py — Codespace 长跑采集 (突破 Actions 6h 限制)

用途:
  1. 补全 K 线覆盖率 (当前 2577/5343 ≈ 48%, 腾讯单次约 1750 次后 501 限流,
     Actions 单次 5min 跑不完 → 放 Codespace 里错峰分批跑数小时)
  2. 分钟级实时采集 (交易时段每 1min 拉一次, Actions cron 最快只能 5min)

设计:
  - 断点续传: 进度写 data/bulk/_longrun_progress.json, 中断后从断点继续
  - 限流自适应: 遇到 501/429 自动退避 (60s → 120s → 300s)
  - 增量写入: 每批立即追加到 day_qfq_recent.csv, 不攒在内存
  - 可中断: Ctrl+C 安全退出, 进度已保存

用法 (Codespace 内):
  python3 scripts/longrun_collect.py kline           # 补K线, 跑到全覆盖
  python3 scripts/longrun_collect.py kline --max-hours 4
  python3 scripts/longrun_collect.py realtime        # 分钟级实时采集 (盘中)
"""
import argparse
import glob
import gzip
import json
import math
import os
import random
import re
import sys
import time
import urllib.request
from datetime import datetime, timedelta, timezone

CST = timezone(timedelta(hours=8))
UA = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120",
      "Referer": "https://gu.qq.com/"}

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
try:
    from paths import BULK
except Exception:
    BULK = os.environ.get("STOCKS_KLINE_BULK") or "/workspaces/stocks-data-kline/data/bulk"
BULK = os.environ.get("STOCKS_KLINE_BULK") or BULK

PROGRESS = os.path.join(BULK, "_longrun_progress.json")
RECENT = os.path.join(BULK, f"day_qfq_longrun_{os.environ['GITHUB_RUN_ID']}.csv.gz"
                      if os.environ.get("GITHUB_RUN_ID") else "day_qfq_recent.csv")
UNIVERSE = os.path.join(BULK, "universe.csv")


def log(msg):
    print(f"[{datetime.now(CST):%H:%M:%S}] {msg}", flush=True)


def load_universe():
    codes = []
    source = os.environ.get("STOCKS_UNIVERSE_JSON") or UNIVERSE
    with open(source, encoding="utf-8") as f:
        if os.environ.get("STOCKS_UNIVERSE_JSON"):
            items = json.load(f)
            if not isinstance(items, list):
                raise ValueError("股票池必须为 list")
            for item in items:
                if (not isinstance(item, list) or len(item) != 3
                        or not all(isinstance(v, str) for v in item)
                        or not re.fullmatch(r"(sh|sz|bj)[0-9]{6}", item[0])
                        or item[0][2:] != item[1]):
                    raise ValueError("股票池存在无效代码或结构")
                codes.append(item[0])
        else:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "|" not in line:
                    raise ValueError("股票池存在无效 CSV 行")
                code = line.split("|")[0].strip()
                if not re.fullmatch(r"(sh|sz|bj)[0-9]{6}", code):
                    raise ValueError("股票池存在无效代码")
                codes.append(code)
    stocks = list(dict.fromkeys(c for c in codes if re.fullmatch(
        r"sh(?:600|601|603|605|688|689)[0-9]{3}|"
        r"sz(?:000|001|002|003|300|301)[0-9]{3}|"
        r"bj(?:43|83|87|88|92)[0-9]{4}", c)))
    if not stocks:
        raise ValueError("股票池为空或不含沪深北普通股票")
    log(f"历史补录股票池: {source}, 来源日期="
        f"{os.environ.get('STOCKS_UNIVERSE_DATE', 'unknown')}; "
        "覆盖率仅限给定股票池，不代表最新全市场")
    return stocks


def parse_day(row):
    if len(row) < 6:
        raise ValueError("日线字段不足")
    datetime.strptime(row[0], "%Y-%m-%d")
    numbers = [float(v) for v in row[1:6]]
    if (not all(math.isfinite(v) for v in numbers)
            or min(numbers[:4]) <= 0 or numbers[4] < 0
            or numbers[2] < max(numbers[0], numbers[1], numbers[3])
            or numbers[3] > min(numbers[0], numbers[1])):
        raise ValueError("日线数值无效")
    return [row[0], *numbers]


def load_covered():
    """已覆盖的代码集合 (所有 bulk csv + recent)"""
    have = set()
    files = (glob.glob(os.path.join(BULK, "day_qfq_*.csv"))
             + glob.glob(os.path.join(BULK, "day_qfq_*.csv.gz")))
    for p in files:
        opener = gzip.open if p.endswith(".gz") else open
        with opener(p, "rt", encoding="utf-8") as f:
            for line in f:
                parts = line.strip().split("|")
                if len(parts) != 7 or parts[0] in have or not re.fullmatch(r"(sh|sz|bj)[0-9]{6}", parts[0]):
                    continue
                try:
                    parse_day(parts[1:])
                except (ValueError, TypeError):
                    continue
                have.add(parts[0])
    return have


def report_coverage():
    universe = set(load_universe())
    have = load_covered() & universe
    log(f"给定股票池 covered={len(have)}/{len(universe)} "
        f"({len(have)/len(universe)*100:.1f}%), missing={len(universe - have)}")
    return 0


def load_progress():
    if os.path.exists(PROGRESS):
        try:
            return json.load(open(PROGRESS, encoding="utf-8"))
        except Exception:
            pass
    return {"done": [], "failed": [], "runs": 0, "last_ts": None}


def save_progress(prog):
    prog["last_ts"] = datetime.now(CST).isoformat(timespec="seconds")
    temporary = PROGRESS + ".tmp"
    with open(temporary, "w", encoding="utf-8") as f:
        json.dump(prog, f, ensure_ascii=False)
    os.replace(temporary, PROGRESS)


def fetch_kline(code, n=120):
    url = (f"https://web.ifzq.gtimg.cn/appstock/app/fqkline/get"
           f"?param={code},day,,,{n},")
    for attempt in range(3):
        try:
            body = urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                          timeout=15).read()
            d = json.loads(body.decode("utf-8"))
            node = (d.get("data") or {}).get(code) or {}
            arr = node.get("day") or []
            rows = []
            for r in arr:
                try:
                    rows.append(parse_day(r))
                except (ValueError, TypeError):
                    continue
            return rows, None if rows else "no_raw_day"
        except urllib.error.HTTPError as e:
            if e.code in (501, 429):
                return [], f"ratelimit_{e.code}"
            return [], f"http_{e.code}"
        except Exception:
            if attempt < 2:
                time.sleep(1)
    return [], "network"


def run_kline(max_hours=4.0, batch_pause=0.8):
    """补K线主循环"""
    t0 = time.time()
    universe = load_universe()
    covered = load_covered() & set(universe)
    prog = load_progress()
    prog["runs"] = prog.get("runs", 0) + 1

    todo = [c for c in universe if c not in covered]
    log(f"universe={len(universe)} covered={len(covered)} todo={len(todo)} "
        f"(第 {prog['runs']} 轮)")

    if not todo:
        log("给定历史股票池已覆盖（至少一条有效原始日线，不代表历史完整）")
        return 0

    backoff = 60
    n_ok = 0
    n_fail = 0
    out = None
    interrupted = False

    try:
        for i, code in enumerate(todo):
            if time.time() - t0 > max_hours * 3600:
                log(f"达到时间上限 {max_hours}h, 退出 (下次继续)")
                break

            rows, err = fetch_kline(code)
            if rows:
                if out is None:
                    opener = gzip.open if RECENT.endswith(".gz") else open
                    out = opener(RECENT, "at", encoding="utf-8")
                for r in rows:
                    out.write(code + "|" + "|".join(map(str, r)) + "\n")
                out.flush()
                n_ok += 1
                backoff = 60
                if code not in prog["done"]:
                    prog["done"].append(code)
            else:
                n_fail += 1
                if err and err.startswith("ratelimit"):
                    log(f"限流 {err}, 退避 {backoff}s")
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 600)
                else:
                    prog["failed"].append(code)
                    time.sleep(batch_pause + random.uniform(0, 0.3))
                save_progress(prog)
                continue

            if n_ok % 100 == 0:
                cov = len(covered) + n_ok
                log(f"进度 {i+1}/{len(todo)}  新增{n_ok}  失败{n_fail}  "
                    f"覆盖率 {cov}/{len(universe)} ({cov/len(universe)*100:.1f}%)")
                save_progress(prog)

            time.sleep(batch_pause + random.uniform(0, 0.3))
    except KeyboardInterrupt:
        interrupted = True
        log("用户中断")
    finally:
        if out is not None:
            out.close()
        save_progress(prog)

    covered2 = load_covered() & set(universe)
    log(f"本轮结束: 新增 {n_ok}, 失败 {n_fail}, 覆盖率 "
        f"{len(covered2)}/{len(universe)} ({len(covered2)/len(universe)*100:.1f}%)")
    return 130 if interrupted else (0 if n_ok else 1)


def run_realtime(interval=60):
    """分钟级实时采集 (交易时段)"""
    log(f"实时采集启动, 每 {interval}s (Ctrl+C 退出)")
    live = os.path.join(os.path.dirname(BULK), "market")
    os.makedirs(live, exist_ok=True)

    def in_session():
        now = datetime.now(CST)
        if now.weekday() >= 5:
            return False
        m = now.hour * 60 + now.minute
        return 555 <= m <= 905

    n = 0
    try:
        while True:
            if not in_session():
                log("非交易时段, 等待...")
                time.sleep(60)
                continue
            codes = load_universe()
            quotes = {}
            for i in range(0, len(codes), 60):
                chunk = codes[i:i + 60]
                url = "https://qt.gtimg.cn/q=" + ",".join(chunk)
                try:
                    body = urllib.request.urlopen(
                        urllib.request.Request(url, headers=UA), timeout=15).read()
                    text = body.decode("gbk", "replace")
                    import re
                    for m in re.finditer(r'v_([a-z0-9]+)="([^"]*)"', text):
                        p = m.group(2).split("~")
                        if len(p) < 40:
                            continue
                        try:
                            quotes[m.group(1)] = {
                                "code": m.group(1), "name": p[1],
                                "price": float(p[3] or 0),
                                "prev_close": float(p[4] or 0),
                                "open": float(p[5] or 0),
                                "high": float(p[33] or 0),
                                "low": float(p[34] or 0),
                                "chg_pct": float(p[32] or 0),
                                "turnover": float(p[38] or 0),
                                "pe": float(p[39] or 0),
                                "mktcap": (float(p[44] or 0)) * 1e8,
                                "floatcap": (float(p[45] or 0)) * 1e8,
                                "pb": float(p[46] or 0),
                                "vol_ratio": 1.0,
                                "time": p[30] or "",
                            }
                        except (ValueError, IndexError):
                            continue
                except Exception as e:
                    log(f"batch {i//60+1} 失败: {e}")
                time.sleep(0.2)

            ds = datetime.now(CST).strftime("%Y-%m-%d")
            with open(os.path.join(live, f"snapshot_{ds}.json"), "w", encoding="utf-8") as f:
                json.dump(quotes, f, ensure_ascii=False)
            n += 1
            log(f"第 {n} 次快照: {len(quotes)} 只 -> snapshot_{ds}.json")
            time.sleep(interval)
    except KeyboardInterrupt:
        log("退出")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode", choices=["kline", "realtime", "coverage"])
    ap.add_argument("--max-hours", type=float, default=4.0)
    ap.add_argument("--interval", type=int, default=60)
    args = ap.parse_args()

    if args.mode == "coverage":
        return report_coverage()
    if args.mode == "kline":
        if not math.isfinite(args.max_hours) or args.max_hours <= 0:
            ap.error("--max-hours 必须为有限正数")
        return run_kline(max_hours=args.max_hours)
    if args.mode == "realtime":
        return run_realtime(interval=args.interval)


if __name__ == "__main__":
    sys.exit(main())
