# -*- coding: utf-8 -*-
"""
이벤트 신호 봇 (페이퍼 트레이딩 기록 + 텔레그램 알림) — 빌드 2026-10-22d
  FR  펀딩비 숏쏠림 롱      (횡보 쪽 · v5 변형 D)
  VS  거래량 폭발 추종       (추세 쪽 · 2026-10-22 사전등록 검증 A·B 합격)

━━━━━━━━━━━━━━━━ 고정 규칙 (페이퍼 기간 중 변경 금지) ━━━━━━━━━━━━━━━━
FR  대상 FR_SYMBOLS 20개
    새 펀딩비 확정 시  직전 180회 대비 중간순위 백분위 ≤ 5%  AND  펀딩비 < 0  → 롱
    같은 코인 보유 중이면 무시 / 48시간 후 청산
    백테스트: 거래당 +50.7bp, 승률 약 52%
VS  대상 VS_SYMBOLS 39개 (검증 A·B 코인 합집합)
    1시간봉 확정 시  거래량 z(log, 직전 50봉) ≥ 3  AND  |종가 변화| ≥ 2×ATR(14)
    → 변화 방향으로 진입 (롱·숏 모두 · 연속 발생 시 첫 봉만) / 12시간 후 청산
    검증: A +72.4bp(p=.002) / B +47.7bp(p=.002), 비용 차감 전
공통  손익 = 가격 + 보유 중 펀딩(받으면 +, 내면 −) − 왕복 11bp
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

구조 (MEME 앱 패턴)
  - app.py 가 start() 호출 → 백그라운드 스레드가 매시 03분 1회 실행
  - Upstash: 상태 전체를 키 1개(fr:state)에 저장. 시간당 SET 1회 + 청산 시 LPUSH 1회 → 월 약 2~3천 커맨드
  - Upstash 불가(한도초과 등)면 '메모리 모드'로 신호·알림은 계속. 복구되면 원본과 병합 후 저장
    (원본 덮어쓰기 없음 · 메모리 모드 중 재시작하면 보유 목록만 초기화)
  - 바이비트는 미국 IP 차단 → Render 리전 Singapore 필요
"""
import json
import math
import os
import threading
import time
import traceback
from datetime import datetime, timedelta, timezone

import requests
from flask import Blueprint, jsonify

# ───────────────── 고정 규칙 ─────────────────
FR_SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "BNBUSDT", "ADAUSDT", "LINKUSDT",
              "AVAXUSDT", "LTCUSDT", "SUIUSDT", "1000PEPEUSDT", "TRXUSDT", "DOTUSDT", "NEARUSDT",
              "HYPEUSDT", "ENAUSDT", "WLDUSDT", "AAVEUSDT", "ZECUSDT"]
VS_SYMBOLS = FR_SYMBOLS + ["OPUSDT", "ARBUSDT", "APTUSDT", "INJUSDT", "TIAUSDT", "SEIUSDT", "FILUSDT",
                           "ATOMUSDT", "ETCUSDT", "BCHUSDT", "UNIUSDT", "ONDOUSDT", "TAOUSDT", "RENDERUSDT",
                           "ICPUSDT", "HBARUSDT", "XLMUSDT", "ORDIUSDT", "JUPUSDT"]
ALL_SYMBOLS = sorted(set(VS_SYMBOLS))

FR_LOOKBACK, FR_PCT, FR_HOLD_MS = 180, 0.05, 48 * 3600 * 1000
VS_VOLZ, VS_ATR_MULT, VS_VOL_N, VS_ATR_N, VS_HOLD_MS = 3.0, 2.0, 50, 14, 12 * 3600 * 1000
FEE_BP = 11.0
EXPECT = {"FR": "+50.7bp · 승률 52%", "VS": "+48~72bp (비용 전)"}
RUN_MINUTE = 3

BYBIT = "https://api.bybit.com"
KST = timezone(timedelta(hours=9))
H_MS = 3600 * 1000
STATE_KEY, CLOSED_KEY = "fr:state", "fr:closed"

fr_bp = Blueprint("funding_bot", __name__)
_state = None
_info = {"last_run": None, "last_error": None, "runs": 0, "started": False, "persist": True}
_lock = threading.Lock()


# ───────────────── 인프라 ─────────────────
def _env(*names):
    for n in names:
        v = os.environ.get(n)
        if v:
            return v
    return None


def r_cmd(*args):
    """(성공여부, 결과). 실패/한도초과를 '키 없음'과 구분."""
    # FR_ 접두사가 있으면 신호봇 전용 DB 사용 (스크리너 한도초과와 분리)
    url = (_env("FR_UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_URL") or "").rstrip("/")
    tok = _env("FR_UPSTASH_REDIS_REST_TOKEN", "UPSTASH_REDIS_REST_TOKEN")
    try:
        r = requests.post(url, headers={"Authorization": f"Bearer {tok}"},
                          json=[str(a) for a in args], timeout=10)
        j = r.json()
        if isinstance(j, dict) and "error" in j:
            return False, j["error"]
        return True, j.get("result")
    except Exception as e:
        return False, str(e)


def telegram(text):
    tok = _env("FR_TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_TOKEN")
    chat = _env("FR_TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID")
    if not (tok and chat):
        print("[FR] 텔레그램 설정 없음:", text)
        return
    try:
        requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                      json={"chat_id": chat, "text": text, "disable_web_page_preview": True}, timeout=10)
    except Exception as e:
        print("[FR] 텔레그램 실패:", e)


def bybit(path, params):
    r = requests.get(BYBIT + path, params=params, timeout=10)
    if r.status_code == 403:
        raise RuntimeError("바이비트 403 — 서버 리전이 미국이면 차단됨 (Singapore 필요)")
    r.raise_for_status()
    js = r.json()
    if js.get("retCode") != 0:
        raise RuntimeError(f"bybit {path}: {js.get('retMsg')}")
    return js["result"]


def funding_history(sym):
    """최근 200회 (과거 → 최신)."""
    lst = bybit("/v5/market/funding/history", {"category": "linear", "symbol": sym, "limit": 200})["list"]
    return sorted((int(x["fundingRateTimestamp"]), float(x["fundingRate"])) for x in lst)


def klines_1h(sym):
    """확정된 1시간봉만 (과거 → 최신). [(ts, o, h, l, c, v)]"""
    lst = bybit("/v5/market/kline", {"category": "linear", "symbol": sym, "interval": "60", "limit": 200})["list"]
    rows = sorted((int(x[0]), *map(float, x[1:6])) for x in lst)
    now = int(time.time() * 1000)
    return [r for r in rows if r[0] + H_MS <= now]


def prices():
    lst = bybit("/v5/market/tickers", {"category": "linear"})["list"]
    return {x["symbol"]: float(x["lastPrice"]) for x in lst if x["symbol"] in ALL_SYMBOLS}


# ───────────────── 지표 ─────────────────
def mid_pct(hist, v):
    lt = sum(1 for h in hist if h < v)
    eq = sum(1 for h in hist if h == v)
    return (lt + 0.5 * eq) / len(hist)


def vs_raw(bars):
    """각 봉의 VS 원신호 (+1/−1/0). 백테스트(mr_alt.make_signals)와 동일 계산."""
    n = len(bars)
    c = [b[4] for b in bars]
    tr = [bars[0][2] - bars[0][3]] + [max(bars[i][2] - bars[i][3], abs(bars[i][2] - c[i - 1]),
                                          abs(bars[i][3] - c[i - 1])) for i in range(1, n)]
    atr = [math.nan] * n
    if n >= VS_ATR_N:
        a = sum(tr[:VS_ATR_N]) / VS_ATR_N
        atr[VS_ATR_N - 1] = a
        for i in range(VS_ATR_N, n):
            a = (tr[i] + (VS_ATR_N - 1) * a) / VS_ATR_N
            atr[i] = a
    lv = [math.log1p(b[5]) for b in bars]
    out = [0] * n
    for i in range(VS_VOL_N + 1, n):
        w = lv[i - VS_VOL_N:i]
        mu = sum(w) / VS_VOL_N
        sd = math.sqrt(sum((x - mu) ** 2 for x in w) / (VS_VOL_N - 1))
        if sd == 0 or math.isnan(atr[i]):
            continue
        chg = c[i] - c[i - 1]
        if (lv[i] - mu) / sd >= VS_VOLZ and abs(chg) >= VS_ATR_MULT * atr[i]:
            out[i] = 1 if chg > 0 else -1
    return out


def fmt_t(ms):
    return datetime.fromtimestamp(ms / 1000, KST).strftime("%m/%d %H:%M") if ms else "-"


def empty_state():
    return {"seen": {}, "vs_bar": 0, "open": {}, "stats": {}, "weekly": "", "bootstrapped": False}


def _st(state, key):
    return state["stats"].setdefault(key, {"n": 0, "sum_bp": 0.0, "wins": 0})


def summary_line(s):
    if not s or not s["n"]:
        return "0건"
    return f"{s['n']}건 · 평균 {s['sum_bp'] / s['n']:+.1f}bp · 승률 {s['wins'] / s['n']:.0%} · 합계 {s['sum_bp'] / 100:+.1f}%p"


def stat_key(pos):
    return pos["strat"] if pos["strat"] == "FR" else f"VS_{'L' if pos['dir'] > 0 else 'S'}"


# ───────────────── 1회 실행 (의존성 주입 → 테스트 가능) ─────────────────
def run_once(state, now_ms, get_hist=funding_history, get_px=prices, get_bars=klines_1h,
             send=telegram, push_closed=None):
    changed = False
    rep = {"opened": [], "closed": [], "errors": []}
    px = get_px()
    hist_cache = {}

    def hist(sym):
        if sym not in hist_cache:
            hist_cache[sym] = get_hist(sym)
        return hist_cache[sym]

    # ① 청산 (FR·VS 공통)
    for key, pos in list(state["open"].items()):
        if now_ms < pos["exit_ms"]:
            continue
        sym, d = pos["symbol"], pos["dir"]
        try:
            exit_px = px[sym]
            px_bp = (exit_px / pos["entry_px"] - 1) * 1e4 * d
            carry = -d * sum(r for t, r in hist(sym) if pos["entry_ms"] < t <= now_ms) * 1e4
            net = px_bp + carry - FEE_BP
            if push_closed:
                push_closed({**pos, "exit_px": exit_px, "exit_real_ms": now_ms, "px_bp": round(px_bp, 1),
                             "carry_bp": round(carry, 1), "net_bp": round(net, 1)})
            del state["open"][key]
            s = _st(state, stat_key(pos))
            s["n"] += 1
            s["sum_bp"] += net
            s["wins"] += int(net > 0)
            changed = True
            rep["closed"].append(key)
            side = "롱" if d > 0 else "숏"
            send(f"[{pos['strat']} {side} 청산] {sym} {net / 100:+.2f}%\n"
                 f"가격 {px_bp / 100:+.2f}% / 펀딩 {carry / 100:+.2f}% / 비용 −0.11%\n"
                 f"{pos['entry_px']} → {exit_px}\n"
                 f"{stat_key(pos)} 누적 {summary_line(s)}")
        except Exception as e:
            rep["errors"].append(f"{key} 청산: {e}")

    first = not state["bootstrapped"]

    # ② FR 신규
    for sym in FR_SYMBOLS:
        try:
            h = hist(sym)
            if len(h) < FR_LOOKBACK // 2 + 1:
                continue
            t_new, r_new = h[-1]
            if t_new <= state["seen"].get(sym, 0):
                continue
            state["seen"][sym] = t_new
            changed = True
            if first:
                continue
            past = [r for _, r in h[:-1]][-FR_LOOKBACK:]
            pct = mid_pct(past, r_new)
            key = f"FR:{sym}"
            if not (pct <= FR_PCT and r_new < 0) or key in state["open"]:
                continue
            state["open"][key] = {"strat": "FR", "symbol": sym, "dir": 1, "entry_ms": now_ms,
                                  "exit_ms": now_ms + FR_HOLD_MS, "entry_px": px[sym],
                                  "funding": r_new, "pct": round(pct, 4)}
            rep["opened"].append(key)
            send(f"[FR 롱 진입] {sym}\n"
                 f"펀딩 {r_new * 100:.4f}% (최근 {len(past)}회 중 하위 {pct:.1%})\n"
                 f"기준가 {px[sym]} · 청산 예정 {fmt_t(now_ms + FR_HOLD_MS)} (48h)\n"
                 f"보유 중 {len(state['open'])}개 · 페이퍼")
        except Exception as e:
            rep["errors"].append(f"FR {sym}: {e}")

    # ③ VS 신규 (직전 확정 1시간봉 기준, 시간당 1회)
    bar_ts = (now_ms // H_MS - 1) * H_MS
    if bar_ts > state.get("vs_bar", 0):
        state["vs_bar"] = bar_ts
        changed = True
        if not first:
            for sym in VS_SYMBOLS:
                try:
                    bars = get_bars(sym)
                    if len(bars) < VS_VOL_N + 3 or bars[-1][0] != bar_ts:
                        continue
                    raw = vs_raw(bars)
                    d, prev = raw[-1], raw[-2]
                    if d == 0 or d == prev:          # 첫 봉만
                        continue
                    key = f"VS:{sym}:{now_ms}"
                    chg = (bars[-1][4] / bars[-2][4] - 1) * 100
                    state["open"][key] = {"strat": "VS", "symbol": sym, "dir": d, "entry_ms": now_ms,
                                          "exit_ms": now_ms + VS_HOLD_MS, "entry_px": px[sym],
                                          "bar_chg": round(chg, 2)}
                    rep["opened"].append(key)
                    send(f"[VS {'롱' if d > 0 else '숏'} 진입] {sym}\n"
                         f"1시간봉 {chg:+.2f}% · 거래량 폭발 (z≥3, 2×ATR 이상)\n"
                         f"기준가 {px[sym]} · 청산 예정 {fmt_t(now_ms + VS_HOLD_MS)} (12h)\n"
                         f"보유 중 {len(state['open'])}개 · 페이퍼")
                except Exception as e:
                    rep["errors"].append(f"VS {sym}: {e}")

    if first and not rep["errors"]:
        state["bootstrapped"] = True
        changed = True
        send(f"[신호봇 시작] FR {len(FR_SYMBOLS)}개 · VS {len(VS_SYMBOLS)}개 감시\n"
             f"매시 {RUN_MINUTE:02d}분 확인 · 규칙 고정(페이퍼)")

    # ④ 주간 요약 (월 09시 KST 이후 첫 실행)
    kst = datetime.fromtimestamp(now_ms / 1000, KST)
    wk = kst.strftime("%G-W%V")
    if kst.weekday() == 0 and kst.hour >= 9 and state["weekly"] != wk:
        state["weekly"] = wk
        changed = True
        S = state["stats"]
        send(f"[주간 요약] {wk}\n"
             f"FR 롱: {summary_line(S.get('FR'))}  (기대 {EXPECT['FR']})\n"
             f"VS 롱: {summary_line(S.get('VS_L'))}\n"
             f"VS 숏: {summary_line(S.get('VS_S'))}  (VS 기대 {EXPECT['VS']})\n"
             f"보유 중 {len(state['open'])}개")
    return changed, rep


# ───────────────── 백그라운드 루프 ─────────────────
def _parse(val):
    try:
        s = {**empty_state(), **(json.loads(val) if val else {})}
        # 구버전(FR 전용) 상태 호환
        for k in list(s["open"]):
            if ":" not in k:
                p = s["open"].pop(k)
                s["open"][f"FR:{k}"] = {"strat": "FR", "dir": 1, **p}
        if "n" in s["stats"]:
            s["stats"] = {"FR": s["stats"]}
        return s
    except Exception:
        return None


def _merge(local, remote):
    """메모리 모드 중 쌓인 상태 + Upstash 원본 병합 (원본 유실 방지)."""
    for k, v in remote["open"].items():
        local["open"].setdefault(k, v)
    for sym, t in remote["seen"].items():
        local["seen"][sym] = max(t, local["seen"].get(sym, 0))
    local["vs_bar"] = max(local.get("vs_bar", 0), remote.get("vs_bar", 0))
    for k, rs in remote["stats"].items():
        ls = _st(local, k)
        for f in ("n", "sum_bp", "wins"):
            ls[f] += rs.get(f, 0)
    local["weekly"] = max(local["weekly"], remote["weekly"])
    local["bootstrapped"] = local["bootstrapped"] or remote["bootstrapped"]


def _sync():
    """메모리 모드 → 저장 모드 복귀 시도. 성공하면 True."""
    ok, val = r_cmd("GET", STATE_KEY)
    if not ok:
        return False
    remote = _parse(val)
    if remote is None:
        return False
    _merge(_state, remote)
    _info["persist"] = True
    print("[신호봇] Upstash 복구 → 병합 후 저장 모드 복귀")
    return True


def _loop():
    global _state
    while True:
        try:
            now = datetime.now(timezone.utc)
            nxt = now.replace(minute=RUN_MINUTE, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(hours=1)
            time.sleep(max(5, (nxt - now).total_seconds()))
            with _lock:
                mem_note = None
                if _state is None:
                    ok, val = r_cmd("GET", STATE_KEY)
                    _state = _parse(val) if ok else None
                    if _state is None:
                        _state = empty_state()
                        _info["persist"] = False
                if not _info["persist"] and not _sync():
                    mem_note = "Upstash 사용 불가 → 메모리 모드 (신호·알림 정상, 재시작 시 보유목록 초기화)"
                changed, rep = run_once(_state, int(time.time() * 1000),
                                        push_closed=lambda rec: r_cmd("LPUSH", CLOSED_KEY, json.dumps(rec)))
                if changed and _info["persist"]:
                    ok, err = r_cmd("SET", STATE_KEY, json.dumps(_state))
                    if not ok:
                        _info["persist"] = False      # 다음 시간에 병합 후 재시도
                        rep["errors"].append(f"상태 저장 실패 → 메모리 모드: {str(err)[:100]}")
                if mem_note:
                    rep["errors"].append(mem_note)
                _info["runs"] += 1
                _info["last_run"] = int(time.time() * 1000)
                _info["last_error"] = "; ".join(rep["errors"])[:500] or None
                if rep["opened"] or rep["closed"] or rep["errors"]:
                    print(f"[신호봇] {rep}")
        except Exception:
            _info["last_error"] = traceback.format_exc()[-500:]
            print("[신호봇] loop error", _info["last_error"])
            time.sleep(60)


def _watch():
    t = None
    while True:
        if t is None or not t.is_alive():
            t = threading.Thread(target=_loop, daemon=True, name="signal_bot")
            t.start()
            print("[신호봇] thread started")
        time.sleep(60)


def start():
    if _info["started"]:
        return
    _info["started"] = True
    threading.Thread(target=_watch, daemon=True, name="signal_bot_watch").start()


# ───────────────── 라우트 (메모리만 · Upstash 안 씀) ─────────────────
@fr_bp.route("/fr/status")
def route_status():
    s = _state
    if s is None:
        return jsonify({"state": "아직 로드 전 (다음 매시 03분에 로드)", **_info})
    return jsonify({
        "FR 롱": summary_line(s["stats"].get("FR")),
        "VS 롱": summary_line(s["stats"].get("VS_L")),
        "VS 숏": summary_line(s["stats"].get("VS_S")),
        "보유": [{"전략": v["strat"], "코인": v["symbol"], "방향": "롱" if v["dir"] > 0 else "숏",
                "진입": fmt_t(v["entry_ms"]), "청산예정": fmt_t(v["exit_ms"]), "진입가": v["entry_px"]}
               for v in sorted(s["open"].values(), key=lambda x: x["exit_ms"])],
        "저장": "Upstash 저장 중" if _info["persist"] else "메모리 모드 (Upstash 불가)",
        "last_run": fmt_t(_info["last_run"]), "runs": _info["runs"], "last_error": _info["last_error"],
    })


@fr_bp.route("/fr/test")
def route_test():
    out = {}
    try:
        out["bybit_btc_funding"] = funding_history("BTCUSDT")[-1]
        out["bybit_btc_last_1h_bar"] = klines_1h("BTCUSDT")[-1][:5]
        out["bybit"] = "ok"
    except Exception as e:
        out["bybit"] = f"실패: {e}"
    try:
        px = prices()
        out["missing_symbols"] = [s for s in ALL_SYMBOLS if s not in px]
    except Exception as e:
        out["prices"] = f"실패: {e}"
    ok, res = r_cmd("EXISTS", STATE_KEY)
    out["upstash"] = "ok" if ok else f"실패: {str(res)[:200]}"
    out["upstash_db"] = "신호봇 전용(FR_)" if _env("FR_UPSTASH_REDIS_REST_URL") else "스크리너와 공용"
    telegram("[신호봇 연결 테스트] 이 메시지가 보이면 텔레그램 OK")
    out["telegram"] = "전송함 — 텔레그램 확인"
    return jsonify(out)
