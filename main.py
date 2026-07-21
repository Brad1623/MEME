# -*- coding: utf-8 -*-
"""
Solana Memecoin Momentum Screener v1.0
=======================================
24시간 솔라나 밈코인 급등 후보 스크리닝 → 텔레그램 알림

데이터 소스 (전부 무료):
  - DexScreener API  : 거래량/가격/매수매도/유동성/부스트
  - GeckoTerminal API: 트렌딩 풀, 신규 풀, 5분봉 OHLCV (EMA 크로스)
  - RugCheck API     : 러그풀 위험 필터 (danger 플래그 자동 제외)
  - Reddit public JSON: 서브레딧 언급량 증가 감지

인프라: Render Background Worker + Upstash Redis (REST) + Telegram Bot
"""

import os
import time
import json
import threading
import traceback
import requests
from datetime import datetime, timezone

# ============================================================
# 설정 (환경변수)
# ============================================================
TG_TOKEN      = os.environ["TELEGRAM_BOT_TOKEN"]
TG_CHAT       = os.environ["TELEGRAM_CHAT_ID"]
UPSTASH_URL   = os.environ["UPSTASH_REDIS_REST_URL"].rstrip("/")
UPSTASH_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

# 필터 임계값 (환경변수로 튜닝 가능)
MIN_LIQ_USD    = float(os.getenv("MIN_LIQUIDITY_USD", "30000"))   # 최소 유동성 $30k (러그/슬리피지 방어)
MIN_VOL24_USD  = float(os.getenv("MIN_VOL24_USD", "100000"))      # 최소 24h 거래량 $100k
MIN_AGE_MIN    = int(os.getenv("MIN_AGE_MINUTES", "20"))          # 상장 20분 미만 제외 (근거는 SHADOW_TRACK로 검증중)
MAX_AGE_DAYS   = int(os.getenv("MAX_AGE_DAYS", "30"))             # 30일 이상 된 코인 제외 (신규 모멘텀 집중)
ALERT_SCORE    = float(os.getenv("ALERT_SCORE", "65"))            # 알림 발송 최소 점수 (0~100)
COOLDOWN_SEC   = int(os.getenv("ALERT_COOLDOWN_SEC", "7200"))     # 동일 코인 재알림 쿨다운 2시간(밈코인 특성상 6h는 과함)
LOOP_SEC       = int(os.getenv("LOOP_SECONDS", "120"))            # 스캔 주기 2분
REDDIT_CYCLES  = int(os.getenv("REDDIT_EVERY_CYCLES", "5"))       # 레딧은 N사이클마다 (rate limit 보호)
OHLCV_TOP_N    = int(os.getenv("OHLCV_TOP_N", "8"))               # EMA 계산은 상위 N개만 (API 절약)

# --- 눌림목(DIP) 감지 설정 ---
DIP_ENABLE     = os.getenv("DIP_ENABLE", "1") == "1"              # dip 알림 on/off
DIP_TRACK_SEC  = int(os.getenv("DIP_TRACK_SEC", "7200"))          # 급등코인 추적 유지 2시간
DIP_TREND_H1   = float(os.getenv("DIP_TREND_H1", "20"))           # 큰추세 조건: 1h 상승 최소 +20%
DIP_M5_MIN     = float(os.getenv("DIP_M5_MIN", "-8"))             # 눌림 하한: 5m -8%까지 (더 깊으면 이탈 위험)
DIP_M5_MAX     = float(os.getenv("DIP_M5_MAX", "-3"))             # 눌림 상한: 5m -3%부터 (얕으면 무시)
DIP_VOL_RATIO  = float(os.getenv("DIP_VOL_RATIO", "0.9"))         # 눌릴때도 거래량 시간평균 90% 이상 유지
DIP_BUY_RATIO  = float(os.getenv("DIP_BUY_RATIO", "0.55"))        # 눌리는 중에도 매수비율 55% 이상 필수
DIP_MAX_FADE   = float(os.getenv("DIP_MAX_FADE", "0.5"))          # 추세붕괴 방지: 현재h1 < 등록시점*0.5면 제외
DIP_COOLDOWN   = int(os.getenv("DIP_COOLDOWN_SEC", "3600"))       # dip 재알림 쿨다운 1시간

# --- 눌림목 강화: 오래된 코인 우대(A) + 레딧 체크(B) ---
DIP_VETERAN_DAYS = float(os.getenv("DIP_VETERAN_DAYS", "7"))       # 이 일수 이상 = '검증된' 오래된 코인
DIP_VETERAN_EASE = float(os.getenv("DIP_VETERAN_EASE", "0.8"))     # 오래된 코인은 거래량/매수 기준을 이 비율로 완화
DIP_REDDIT_CHECK = os.getenv("DIP_REDDIT_CHECK", "1") == "1"       # 눌림목 레딧 언급 체크 on/off

# --- 매도 시그널 (포지션 감시) — 가중치 점수제 ---
SELL_SCORE     = float(os.getenv("SELL_SCORE", "40"))              # 위험도 이 점수 이상이면 알림 (0~100)
SELL_BUY_RATIO = float(os.getenv("SELL_BUY_RATIO", "0.45"))        # 매수비율 이 밑 = 매수세 붕괴
SELL_VOL_RATIO = float(os.getenv("SELL_VOL_RATIO", "0.5"))         # 거래량 기준 (시장 활성도로 자동 보정)
SELL_CRASH_M5  = float(os.getenv("SELL_CRASH_M5", "-15"))          # 5분 이 이하 하락 = 급락 경보
SELL_TP_PCT    = float(os.getenv("SELL_TP_PCT", "20"))             # 진입 대비 이 수익률 도달 = 익절 검토
SELL_SIG_CD    = int(os.getenv("SELL_SIG_COOLDOWN", "1800"))       # 매도알림 재발송 쿨다운 30분
SELL_CRASH_CD  = int(os.getenv("SELL_CRASH_COOLDOWN", "900"))      # 급락 포함 시 15분
SELL_ESCALATE  = float(os.getenv("SELL_ESCALATE", "20"))           # 위험도가 이만큼 급등하면 쿨다운 무시하고 재알림
SELL_SL_PCT    = float(os.getenv("SELL_SL_PCT", "20"))             # 기본 손절선: 진입 대비 -20% (최우선 알림)

# --- 알림 성과 자동추적 (+10% 가설 검증) ---
PERF_ENABLE    = os.getenv("PERF_ENABLE", "1") == "1"              # 알림코인 24h 성과추적 on/off

# --- 20분 미만 코인 섀도우 추적 (나이 필터 근거 검증용, 알림 없이 조용히 perf만 기록) ---
SHADOW_ENABLE  = os.getenv("SHADOW_ENABLE", "1") == "1"
SHADOW_TTL     = int(os.getenv("SHADOW_TTL_SEC", "10800"))         # 중복 추적 방지 3시간

# --- 2단 눌림(더블딥) 관찰 트래커 (알림 없이 검증 전용, 2026-07-21) ---
# 가설: 신생코인이 1차 얕은 눌림(-10%) 후 반등, 다시 2차 깊은 눌림(-30%)이 오면
#       그 시점이 좋은 진입점이다 — 아직 검증 안 됨, forward-test로 데이터부터 쌓음
DOUBLEDIP_ENABLE   = os.getenv("DOUBLEDIP_ENABLE", "1") == "1"
DOUBLEDIP_MC_MIN   = float(os.getenv("DOUBLEDIP_MC_MIN", "500000"))  # 대상 하한: 시총 $500K (전문가 스위트스팟 하한)
DOUBLEDIP_D1_PCT   = float(os.getenv("DOUBLEDIP_D1_PCT", "10"))      # 1차 눌림: 고점 대비 -10% 이내
DOUBLEDIP_D2_PCT   = float(os.getenv("DOUBLEDIP_D2_PCT", "30"))      # 2차 눌림: 반등고점 대비 -30% 수준
DOUBLEDIP_WINDOW_S = int(os.getenv("DOUBLEDIP_WINDOW_SEC", "3600"))  # 1차→2차 시간제한 1시간
DOUBLEDIP_STATE_TTL = int(os.getenv("DOUBLEDIP_STATE_TTL_SEC", "7200"))  # 상태 보관 2시간(윈도우+여유)

# --- 원클릭 매수 버튼 ---
# 알림에 표시할 SOL 매수 금액 세트 (쉼표 구분). 누르면 Jupiter 스왑 페이지가 해당 금액으로 열림
BUY_AMOUNTS = [a.strip() for a in os.getenv("BUY_AMOUNTS_SOL", "0.5,1,2").split(",") if a.strip()]
SOL_MINT    = "So11111111111111111111111111111111111111112"

# --- 전문가 기준 태그 (전부 태그만, 알림 차단 안 함. 끄려면 =0) ---
TAG_ENABLE       = os.getenv("TAG_ENABLE", "1") == "1"            # 전문가 태그 전체 on/off
# 스위트스팟 시가총액 (달러). 전문가: 코어 1M~5M, 확장 500K~10M
TAG_MC_CORE_LOW  = float(os.getenv("TAG_MC_CORE_LOW", "1000000"))
TAG_MC_CORE_HIGH = float(os.getenv("TAG_MC_CORE_HIGH", "5000000"))
TAG_MC_EXT_LOW   = float(os.getenv("TAG_MC_EXT_LOW", "500000"))
TAG_MC_EXT_HIGH  = float(os.getenv("TAG_MC_EXT_HIGH", "10000000"))
# 1시간 트랜잭션 건수 기준 (전문가: 250+)
TAG_MIN_TXNS     = int(os.getenv("TAG_MIN_TXNS", "250"))
# 내부자/홀더 집중도 경고선 (전문가: 졸업완료 16%, 임박 20%, 50%+ 확정사기)
TAG_INSIDER_WARN = float(os.getenv("TAG_INSIDER_WARN", "20"))
TAG_INSIDER_DANGER = float(os.getenv("TAG_INSIDER_DANGER", "50"))
# 봇차트 의심: 나이(분) 대비 급등 판정 — 어린데 이미 급등 = 봇 의심
TAG_BOT_AGE_MIN  = float(os.getenv("TAG_BOT_AGE_MIN", "20"))      # 20분 미만인데
TAG_BOT_PUMP_H1  = float(os.getenv("TAG_BOT_PUMP_H1", "100"))     # 1h +100%+ 면 봇 의심

UA = {"User-Agent": "Mozilla/5.0 (sol-meme-screener/1.0)"}
SUBREDDITS = ["CryptoMoonShots", "solana", "memecoins", "SolanaMemeCoins"]

DEXS_BASE  = "https://api.dexscreener.com"
GECKO_BASE = "https://api.geckoterminal.com/api/v2"
RUG_BASE   = "https://api.rugcheck.xyz/v1"


# ============================================================
# Upstash Redis REST 헬퍼
# ============================================================
def r_cmd(*args):
    """Upstash REST로 Redis 명령 실행. 실패 시 None (봇은 계속 동작)."""
    try:
        resp = requests.post(
            UPSTASH_URL,
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=[str(a) for a in args],
            timeout=10,
        )
        return resp.json().get("result")
    except Exception as e:
        print(f"[redis] error: {e}")
        return None


def r_pipe(commands):
    """여러 Redis 명령을 파이프라인 1회 요청으로 실행 (커맨드 수 절감 핵심).
    commands: [["GET","k1"], ["GET","k2"], ...]. 반환: [result1, result2, ...].
    실패 시 [None]*len(commands)."""
    if not commands:
        return []
    try:
        resp = requests.post(
            UPSTASH_URL + "/pipeline",
            headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"},
            json=[[str(a) for a in cmd] for cmd in commands],
            timeout=15,
        )
        out = resp.json()
        return [r.get("result") if isinstance(r, dict) else None for r in out]
    except Exception as e:
        print(f"[redis] pipeline error: {e}")
        return [None] * len(commands)


def archive_alert(kind, sym, score, pair, tag_verdict=None):
    """발송된 알림을 아카이브 리스트에 기록 (최근 50개 유지).
    kind: 'pump'(급등) / 'dip'(눌림목).
    형식: 'kind|sym|score|판정|가격변동요약|UTC시각' 파이프 구분."""
    try:
        pc = pair.get("priceChange") or {}
        h1 = pc.get("h1", "?")
        h24 = pc.get("h24", "?")
        verdict = (tag_verdict or "").split(" (")[0] if tag_verdict else "-"
        ts = datetime.now(timezone.utc).strftime("%m-%d %H:%M")
        rec = f"{kind}|{sym}|{score}|{verdict}|1h {h1}%·24h {h24}%|{ts}"
        r_cmd("LPUSH", "archive:alerts", rec)
        r_cmd("LTRIM", "archive:alerts", 0, 49)
    except Exception as e:
        print(f"[archive] error: {e}")


# ============================================================
# 포지션 관리 + 매도 시그널
# ============================================================
def market_activity(pairs):
    """시장 전체 활성도 지수. 지금 median 거래량비 ÷ 최근 24h 시간대별 baseline.
    반환: 0.3~2.0 (1.0=평시, 0.5=한산 → 미국새벽 등).
    baseline 부족(3시간 미만)이면 1.0 반환 = 보정 안 함.
    ★ Redis 절감: baseline 조회+락체크를 파이프라인 1회로, 저장도 묶음."""
    ratios = []
    for p in pairs.values():
        vol = p.get("volume") or {}
        v1 = float(vol.get("h1") or 0)
        v24 = float(vol.get("h24") or 0)
        if v24 > 0 and v1 > 0:
            ratios.append(v1 / (v24 / 24))
    if len(ratios) < 5:
        return 1.0
    ratios.sort()
    now_med = ratios[len(ratios) // 2]

    # baseline 리스트 + 시간락을 1회 파이프라인으로 조회
    res = r_pipe([["LRANGE", "mkt:base", 0, 23], ["GET", "mkt:hourlock"]])
    base_list = res[0] or []
    hourlock = res[1]

    # 시간당 1샘플 축적 (락 없을 때만) — 저장 명령들 묶음
    writes = [["SET", "mkt:now", round(now_med, 3)]]
    if not hourlock:
        writes.append(["SETEX", "mkt:hourlock", 3300, "1"])
        writes.append(["LPUSH", "mkt:base", round(now_med, 3)])
        writes.append(["LTRIM", "mkt:base", 0, 23])

    vals = []
    for x in base_list:
        try:
            vals.append(float(x))
        except (ValueError, TypeError):
            pass
    act = 1.0
    if len(vals) >= 3:
        vals.sort()
        base = vals[len(vals) // 2]
        if base > 0:
            act = max(0.3, min(2.0, now_med / base))
            writes.append(["SET", "mkt:activity", round(act, 2)])
    r_pipe(writes)
    return act


def check_sell(pair, entry=None, activity=1.0):
    """매도 위험도 점수화 (0~100) — 신호별 가중치 + 시장 활성도 보정.
    activity: 시장 활성도 (1.0=평시, 0.5=한산). 거래량 신호의 기준·배점을 함께 낮춤.
    반환: (score, sigs, pnl, price, tp_hit). sigs=[(키, 설명, 배점)]"""
    pc = pair.get("priceChange") or {}
    vol = pair.get("volume") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}
    m5 = float(pc.get("m5") or 0)
    h1 = float(pc.get("h1") or 0)
    v1 = float(vol.get("h1") or 0)
    v24 = float(vol.get("h24") or 0)
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    price = float(pair.get("priceUsd") or 0)
    pnl = ((price - entry) / entry * 100) if (entry and entry > 0 and price > 0) else None

    score = 0.0
    sigs = []

    # ① 급락 (최대 50) — 시간대 무관한 절대 신호. 깊을수록 가중.
    if m5 <= SELL_CRASH_M5:
        w = min(50.0, 45.0 * abs(m5) / abs(SELL_CRASH_M5))
        score += w
        sigs.append(("crash", f"🚨 급락 (5m {m5:.1f}%)", w))

    # ② 매수세 붕괴 (최대 25) — '비율'이라 시간대 영향 적음 (밤엔 매수·매도 같이 줄어듦)
    if buys + sells >= 30:
        br = buys / (buys + sells)
        if br < SELL_BUY_RATIO:
            w = 25.0 * min(1.0, (SELL_BUY_RATIO - br) / 0.20)
            score += w
            sigs.append(("buyfail", f"📉 매수세 붕괴 (매수 {br*100:.0f}%)", w))

    # ③ 거래량 소멸 (최대 20) — ★시장 활성도로 기준·배점 동시 보정★
    #    한산한 시간대엔 모든 코인 거래량이 주니까, 기준을 낮추고 배점도 줄인다.
    if v24 > 0:
        vr = v1 / (v24 / 24)
        thr = SELL_VOL_RATIO * activity
        if thr > 0 and vr < thr:
            w = 20.0 * min(1.0, (thr - vr) / thr) * min(1.0, activity)
            score += w
            note = f"💤 거래량 소멸 ({vr:.1f}x < 기준 {thr:.1f}x)"
            if activity < 0.8:
                note += " ※한산시간 보정"
            sigs.append(("voldry", note, w))

    # ④ 모멘텀 반전 (최대 15) — 5m·1h 동시 하락, 깊을수록 가중
    if m5 < 0 and h1 < 0:
        w = 15.0 * min(1.0, (abs(m5) + abs(h1)) / 20.0)
        score += w
        sigs.append(("momrev", f"↩️ 모멘텀 반전 (5m {m5:.1f}% · 1h {h1:.1f}%)", w))

    tp_hit = pnl is not None and pnl >= SELL_TP_PCT
    return round(min(100.0, score), 1), sigs, pnl, price, tp_hit


def positions_tick(pairs, activity=1.0):
    """매 사이클 포지션 감시: 위험도 점수 알림 + 익절 알림 + 스냅샷 기록."""
    mints = r_cmd("SMEMBERS", "pos:index") or []
    if not mints:
        r_cmd("SET", "pos:snapshot", "[]")
        return
    # 포지션 정보 + 손절 쿨다운을 1회 파이프라인으로 조회
    raws = r_pipe([["GET", f"pos:{m}"] for m in mints]
                  + [["GET", f"slcd:{m}"] for m in mints])
    pos_raws = raws[:len(mints)]
    slcd_flags = raws[len(mints):]
    snapshot = []
    stale = []
    for idx, mint in enumerate(mints):
        raw = pos_raws[idx]
        if not raw:
            stale.append(["SREM", "pos:index", mint])
            continue
        try:
            pos = json.loads(raw)
        except Exception:
            stale.append(["SREM", "pos:index", mint])
            continue
        pair = pairs.get(mint)
        if not pair:
            snapshot.append({"sym": pos.get("sym", "?"), "entry": pos.get("entry"),
                             "price": None, "pnl": None, "risk": None})
            continue

        score, sigs, pnl, price, tp_hit = check_sell(pair, pos.get("entry"), activity)
        sym = pos.get("sym", "?")
        pnl_txt = (f"\n💰 진입 ${pos.get('entry'):g} → 현재 ${price:g} (<b>{pnl:+.1f}%</b>)"
                   if pnl is not None else "")

        # 🛑 손절선 알림 (최우선 — 위험도 점수와 무관, 15분 쿨다운)
        sl = float(pos.get("sl") or SELL_SL_PCT)
        if pnl is not None and pnl <= -sl and not slcd_flags[idx]:
            r_cmd("SETEX", f"slcd:{mint}", SELL_CRASH_CD, "1")
            tg_send(f"🛑 <b>[손절선 도달] {sym}</b>  손절기준 -{sl:g}%{pnl_txt}\n"
                    f"규칙대로면 지금 정리. 위험도 {score:.0f}/100\n"
                    f'📊 <a href="https://dexscreener.com/solana/{pair.get("pairAddress","")}">차트</a>',
                    buttons=[("✅ 매도 완료 (감시종료+기록)", f"sold:{mint}")])
            print(f"[SL] {sym} {pnl:.1f}%")

        # 익절 알림 (위험이 아니라 '기회' → 별도 트랙)
        if tp_hit and not r_cmd("GET", f"tpcd:{mint}"):
            r_cmd("SETEX", f"tpcd:{mint}", SELL_SIG_CD, "1")
            tg_send(f"🎯 <b>[익절 검토] {sym}</b>  +{pnl:.0f}% 도달{pnl_txt}\n"
                    f"위험도 {score:.0f}/100 · 목표 {SELL_TP_PCT:.0f}%\n"
                    f'📊 <a href="https://dexscreener.com/solana/{pair.get("pairAddress","")}">차트</a>',
                    buttons=[("✅ 매도 완료 (감시종료+기록)", f"sold:{mint}")])
            print(f"[TP] {sym} +{pnl:.0f}%")

        # 위험도 알림 (점수 문턱 + 급등 시 쿨다운 무시)
        if score >= SELL_SCORE:
            prev = r_cmd("GET", f"sellcd:{mint}")
            prev_score = None
            if prev:
                try:
                    prev_score = float(prev)
                except (ValueError, TypeError):
                    prev_score = None
            escalated = prev_score is not None and score >= prev_score + SELL_ESCALATE
            if prev_score is None or escalated:
                cd = SELL_CRASH_CD if any(k == "crash" for k, _, _ in sigs) else SELL_SIG_CD
                r_cmd("SETEX", f"sellcd:{mint}", cd, score)
                lines = "\n".join(f"  {t}  <b>+{w:.0f}</b>" for _, t, w in
                                  sorted(sigs, key=lambda x: -x[2]))
                esc = " ⚠️악화" if escalated else ""
                tg_send(
                    f"🔻 <b>[매도 시그널] {sym}</b>  위험도 <b>{score:.0f}/100</b>{esc}{pnl_txt}\n"
                    f"━━━ 발동 {len(sigs)}/4 ━━━\n{lines}\n"
                    f"━━━━━━━━━━━━━━━\n"
                    f"시장 활성도 {activity:.2f} · 알림기준 {SELL_SCORE:.0f}점\n"
                    f'📊 <a href="https://dexscreener.com/solana/{pair.get("pairAddress","")}">차트</a>',
                    buttons=[("✅ 매도 완료 (감시종료+기록)", f"sold:{mint}")]
                )
                print(f"[SELL-SIG] {sym} score={score} sigs={[k for k,_,_ in sigs]}")

        snapshot.append({"sym": sym, "entry": pos.get("entry"), "price": price,
                         "pnl": round(pnl, 1) if pnl is not None else None,
                         "risk": score})
    r_pipe(stale + [["SET", "pos:snapshot", json.dumps(snapshot)]])


# ============================================================
# 알림 성과 자동추적 (+10% 가설 검증)
# ============================================================
def perf_add(mint, sym, kind, price):
    """알림 발송 시 24h 성과추적 등록."""
    if not PERF_ENABLE or not price or price <= 0:
        return
    ts = int(time.time())
    key = f"perf:p:{mint}:{ts}"
    item = {"mint": mint, "sym": sym, "kind": kind, "ap": price, "ts": ts,
            "peak": 0.0, "trough": 0.0, "r1": None, "r6": None, "r24": None}
    r_cmd("SET", key, json.dumps(item))
    r_cmd("SADD", "perf:index", key)


DEEP_CRASH_EXIT_PCT = -80.0   # 이 낙폭 이하로 빠지면 조기종료 (24h까지 안 기다림 — 대부분 이 밑에서 안 살아남음)
DEEP_CRASH_MIN_AGE_S = 1800   # 초반 변동성 노이즈 방지: 최소 30분 지난 뒤부터 조기종료 판정


def perf_tick(pairs):
    """추적중 코인 갱신: 최고/최저 업데이트, 1h/6h/24h 수익률 확정, 완료 시 집계.
    ★ Redis 절감: 개별 GET/SET을 파이프라인으로 일괄 처리 + 깊은 붕괴 조기종료로 추적기간 단축."""
    if not PERF_ENABLE:
        return
    keys = r_cmd("SMEMBERS", "perf:index") or []
    now = int(time.time())
    if not keys:
        _perf_summarize()
        return

    # ① 전체 항목 1회 파이프라인으로 조회 (기존: 항목당 GET N회 → 1회)
    raws = r_pipe([["GET", k] for k in keys])

    writes = []       # 갱신 SET 묶음
    cleanups = []     # 완료/무효 항목 정리 묶음
    done_push = []    # perf:done 추가 묶음
    for k, raw in zip(keys, raws):
        if not raw:
            cleanups.append(["SREM", "perf:index", k])
            continue
        try:
            it = json.loads(raw)
        except Exception:
            cleanups.append(["SREM", "perf:index", k])
            continue
        pair = pairs.get(it["mint"])
        age = now - it["ts"]
        if pair:
            price = float(pair.get("priceUsd") or 0)
            if price > 0 and it["ap"] > 0:
                pnl = (price - it["ap"]) / it["ap"] * 100
                it["peak"] = round(max(it["peak"], pnl), 1)
                it["trough"] = round(min(it["trough"], pnl), 1)
                if it["r1"] is None and age >= 3600:
                    it["r1"] = round(pnl, 1)
                if it["r6"] is None and age >= 21600:
                    it["r6"] = round(pnl, 1)
                if it["r24"] is None and age >= 86400:
                    it["r24"] = round(pnl, 1)

        # 조기종료: 30분 지나고 -80% 이하로 빠지면 24h 안 기다리고 바로 종료
        # (Upstash 부담 절감 — 이 낙폭에서 반등해 살아나는 경우는 극히 드묾)
        deep_crash = (age >= DEEP_CRASH_MIN_AGE_S and it["trough"] <= DEEP_CRASH_EXIT_PCT)
        if deep_crash and it["r24"] is None:
            it["r24"] = it["trough"]     # 사실상 바닥이므로 trough를 r24 대용으로 기록
            it["early_exit"] = "deep_crash"

        if it["r24"] is not None or age > 90000:
            done_push.append(["LPUSH", "perf:done", json.dumps(it)])
            cleanups.append(["SREM", "perf:index", k])
            cleanups.append(["DEL", k])
        else:
            writes.append(["SET", k, json.dumps(it)])

    # ② 갱신·완료·정리를 파이프라인으로 일괄 실행
    batch = writes + done_push
    if done_push:
        batch.append(["LTRIM", "perf:done", 0, 199])
    batch += cleanups
    if batch:
        r_pipe(batch)

    _perf_summarize()


def _perf_summarize():
    """완료 기록으로 요약 재계산 (alert vs shadow 분리). perf_tick에서 호출."""
    done = r_cmd("LRANGE", "perf:done", 0, 199) or []
    if not done:
        return
    items = []
    for x in done:
        try:
            items.append(json.loads(x))
        except Exception:
            pass
    if True:
        def med(a):
            a = sorted(v for v in a if v is not None)
            return a[len(a)//2] if a else None

        def summarize(subset):
            if not subset:
                return None
            n = len(subset)
            hit10 = sum(1 for i in subset if i.get("peak", 0) >= 10)
            early = sum(1 for i in subset if i.get("early_exit") == "deep_crash")
            return {
                "n": n,
                "hit10_pct": round(hit10 / n * 100),
                "med_r1": med([i.get("r1") for i in subset]),
                "med_r6": med([i.get("r6") for i in subset]),
                "med_r24": med([i.get("r24") for i in subset]),
                "avg_peak": round(sum(i.get("peak", 0) for i in subset) / n, 1),
                "worst": round(min(i.get("trough", 0) for i in subset), 1),
                "early_pct": round(early / n * 100),
            }

        alert_items = [i for i in items if i.get("kind") in ("pump", "dip")]
        shadow_items = [i for i in items if i.get("kind") == "shadow"]
        dd_items = [i for i in items if i.get("kind") == "doubledip"]
        s1 = summarize(alert_items)
        s2 = summarize(shadow_items)
        s3 = summarize(dd_items)
        sets = []
        if s1:
            sets.append(["SET", "perf:summary", json.dumps(s1)])
        if s2:
            sets.append(["SET", "perf:summary_shadow", json.dumps(s2)])
        if s3:
            sets.append(["SET", "perf:summary_doubledip", json.dumps(s3)])
        if sets:
            r_pipe(sets)


# ============================================================
# 텔레그램 명령 수신 (/buy /sell /pos /check)
# ============================================================
def handle_command(text):
    """명령 파싱·실행. 반환: 응답 문자열 (없으면 None)."""
    parts = text.split()
    cmd = parts[0].lower().split("@")[0]

    if cmd in ("/help", "/start"):
        return ("🤖 <b>명령어</b>\n"
                "/buy 민트 [진입가] [손절%] — 등록·감시 시작 (기본 손절 -" + f"{SELL_SL_PCT:g}" + "%)\n"
                "/sell 민트 — 감시 종료 + 손익·누적통계\n"
                "/pos — 보유 포지션 목록\n"
                "/check 민트 — 즉석 매도판단 리포트")

    if cmd == "/buy":
        if len(parts) < 2:
            return "사용법: /buy 민트주소 [진입가] [손절%]"
        mint = parts[1]
        pair = fetch_pairs([mint]).get(mint)
        if not pair:
            return "❌ 코인 조회 실패 — 민트주소를 확인해줘"
        price = float(pair.get("priceUsd") or 0)
        if len(parts) >= 3:
            try:
                price = float(parts[2])
            except ValueError:
                pass
        sl = SELL_SL_PCT
        if len(parts) >= 4:
            try:
                sl = abs(float(parts[3]))
            except ValueError:
                pass
        sym = (pair.get("baseToken") or {}).get("symbol", "?")
        r_cmd("SET", f"pos:{mint}", json.dumps(
            {"sym": sym, "entry": price, "ts": int(time.time()), "sl": sl}))
        r_cmd("SADD", "pos:index", mint)
        return (f"📌 포지션 등록: <b>{sym}</b> @ ${price:g}\n"
                f"🛑 손절선 -{sl:g}% · 🎯 익절선 +{SELL_TP_PCT:g}%\n"
                f"2분 주기 매도감시 시작. 종료는 /sell {mint}")

    if cmd == "/sell":
        if len(parts) < 2:
            return "사용법: /sell 민트주소"
        mint = parts[1]
        raw = r_cmd("GET", f"pos:{mint}")
        if not raw:
            return "해당 민트의 포지션이 없어"
        pos = json.loads(raw)
        pair = fetch_pairs([mint]).get(mint)
        pnl = None
        exit_price = None
        if pair:
            exit_price = float(pair.get("priceUsd") or 0)
            e = pos.get("entry") or 0
            if exit_price > 0 and e > 0:
                pnl = round((exit_price - e) / e * 100, 1)
        r_cmd("SREM", "pos:index", mint)
        r_cmd("DEL", f"pos:{mint}")

        # 실거래 기록 저장 (자기교정 데이터)
        hold_min = int((time.time() - pos.get("ts", time.time())) / 60)
        trade = {"sym": pos.get("sym", "?"), "mint": mint, "entry": pos.get("entry"),
                 "exit": exit_price, "pnl": pnl, "hold_min": hold_min,
                 "ts": datetime.now(timezone.utc).strftime("%m-%d %H:%M")}
        r_cmd("LPUSH", "trades:done", json.dumps(trade))
        r_cmd("LTRIM", "trades:done", 0, 99)

        # 누적 통계 재계산
        stats_txt = ""
        recs = r_cmd("LRANGE", "trades:done", 0, 99) or []
        pnls = []
        for x in recs:
            try:
                p = json.loads(x).get("pnl")
                if p is not None:
                    pnls.append(float(p))
            except Exception:
                pass
        if pnls:
            n = len(pnls)
            wins = [p for p in pnls if p > 0]
            losses = [p for p in pnls if p <= 0]
            wr = round(len(wins) / n * 100)
            aw = round(sum(wins) / len(wins), 1) if wins else 0
            al = round(sum(losses) / len(losses), 1) if losses else 0
            summary = {"n": n, "win_rate": wr, "avg_win": aw, "avg_loss": al,
                       "total": round(sum(pnls), 1)}
            r_cmd("SET", "trades:summary", json.dumps(summary))
            stats_txt = (f"\n📊 누적 {n}건 · 승률 <b>{wr}%</b> · "
                         f"평균익 +{aw}% / 평균손 {al}%")

        pnl_txt = ""
        if pnl is not None:
            pnl_txt = f"\n💰 ${pos.get('entry'):g} → ${exit_price:g} (<b>{pnl:+.1f}%</b>) · 보유 {hold_min}분"
        return f"✅ 감시 종료: {pos.get('sym','?')}{pnl_txt}{stats_txt}"

    if cmd == "/pos":
        raw = r_cmd("GET", "pos:snapshot")
        if not raw or raw == "[]":
            return "보유 포지션 없음. /buy 민트주소 로 등록해"
        try:
            snap = json.loads(raw)
        except Exception:
            return "스냅샷 파싱 오류"
        lines = ["💼 <b>보유 포지션</b>"]
        for p in snap:
            if p.get("pnl") is not None:
                lines.append(f"• {p['sym']}: ${p['entry']:g} → ${p['price']:g} (<b>{p['pnl']:+.1f}%</b>)")
            else:
                lines.append(f"• {p['sym']}: ${p.get('entry','?')} → 시세조회중")
        return "\n".join(lines)

    if cmd == "/check":
        if len(parts) < 2:
            return "사용법: /check 민트주소"
        mint = parts[1]
        pair = fetch_pairs([mint]).get(mint)
        if not pair:
            return "❌ 코인 조회 실패"
        raw = r_cmd("GET", f"pos:{mint}")
        entry = json.loads(raw).get("entry") if raw else None
        act_raw = r_cmd("GET", "mkt:activity")
        try:
            act = float(act_raw) if act_raw else 1.0
        except (ValueError, TypeError):
            act = 1.0
        score, sigs, pnl, price, tp_hit = check_sell(pair, entry, act)
        sym = (pair.get("baseToken") or {}).get("symbol", "?")
        pc = pair.get("priceChange") or {}
        head = f"🔎 <b>{sym}</b> 매도판단  위험도 <b>{score:.0f}/100</b>\n💰 ${price:g}"
        if pnl is not None:
            head += f" (진입대비 <b>{pnl:+.1f}%</b>)"
        head += f"\n📈 5m {pc.get('m5','?')}% · 1h {pc.get('h1','?')}%"
        head += f"\n🌐 시장 활성도 {act:.2f} ({'한산' if act < 0.8 else '평시' if act < 1.3 else '활발'})"
        if tp_hit:
            head += f"\n🎯 목표수익 도달 (+{SELL_TP_PCT:.0f}% 이상)"
        if sigs:
            body = "\n".join(f"  {t}  <b>+{w:.0f}</b>" for _, t, w in sorted(sigs, key=lambda x: -x[2]))
            verdict = "🔴 매도 검토" if score >= SELL_SCORE else "🟡 주의 관찰"
            return f"{head}\n\n━━━ 발동 {len(sigs)}/4 ━━━\n{body}\n━━━━━━━━━━━━━━━\n{verdict} (알림기준 {SELL_SCORE:.0f}점)"
        return f"{head}\n\n✅ 매도 신호 없음 — 추세 유지 중"

    return None


_POLLER_STARTED = False

def tg_poll_loop():
    """텔레그램 명령 폴러 (5초 주기, 별도 스레드)."""
    print("[poller] telegram command poller started")
    while True:
        try:
            params = {"timeout": 0}
            off = r_cmd("GET", "tg:offset")
            if off:
                params["offset"] = int(off) + 1
            resp = requests.get(
                f"https://api.telegram.org/bot{TG_TOKEN}/getUpdates",
                params=params, timeout=10,
            )
            if resp.status_code == 409:
                time.sleep(10)  # 배포 교체 중 이중 폴링 충돌 → 대기
                continue
            for upd in (resp.json().get("result") or []):
                r_cmd("SET", "tg:offset", upd["update_id"])

                # 버튼 클릭 (callback_query) 처리
                cq = upd.get("callback_query")
                if cq:
                    # 로딩 스피너 제거 (필수 응답)
                    try:
                        requests.post(
                            f"https://api.telegram.org/bot{TG_TOKEN}/answerCallbackQuery",
                            json={"callback_query_id": cq["id"]}, timeout=10)
                    except Exception:
                        pass
                    cmsg = cq.get("message") or {}
                    if str((cmsg.get("chat") or {}).get("id")) != str(TG_CHAT):
                        continue
                    data = cq.get("data") or ""
                    reply = None
                    if data.startswith("reg:"):
                        reply = handle_command(f"/buy {data[4:]}")
                    elif data.startswith("sold:"):
                        reply = handle_command(f"/sell {data[5:]}")
                    if reply:
                        tg_send(reply)
                    continue

                msg = upd.get("message") or {}
                if str((msg.get("chat") or {}).get("id")) != str(TG_CHAT):
                    continue
                text = (msg.get("text") or "").strip()
                if text.startswith("/"):
                    reply = handle_command(text)
                    if reply:
                        tg_send(reply)
        except Exception as e:
            print(f"[poller] error: {e}")
        time.sleep(5)


# ============================================================
# 텔레그램
# ============================================================
def tg_send(text, retries=3, detect_ts=None, buttons=None):
    """텔레그램 발송. 실제 성공 여부 확인 + 실패 시 재시도.
    detect_ts: 알림 감지 시각 → '{DELAY}' 자리에 지연 배지.
    buttons: [(라벨, callback_data)] → 알림 아래 인라인 버튼.
    반환: True(성공) / False(최종 실패)."""
    for attempt in range(retries):
        try:
            # 발송 직전에 지연 배지 계산 (재시도로 시간이 흐른 만큼 반영)
            body = text
            if "{DELAY}" in text:
                if detect_ts is not None:
                    delay = time.time() - detect_ts
                    if delay >= 30:
                        badge = f"⏱ 지연발송 (+{delay:.0f}초, 실시간 아님)"
                    elif delay >= 10:
                        badge = f"🕐 약간 지연 (+{delay:.0f}초)"
                    else:
                        badge = "⚡ 실시간"
                    body = text.replace("{DELAY}", badge)
                else:
                    body = text.replace("{DELAY}", "")

            payload = {
                "chat_id": TG_CHAT,
                "text": body,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            }
            if buttons:
                payload["reply_markup"] = {"inline_keyboard": [
                    [{"text": lbl, "callback_data": cb} for lbl, cb in buttons]
                ]}
            resp = requests.post(
                f"https://api.telegram.org/bot{TG_TOKEN}/sendMessage",
                json=payload,
                timeout=10,
            )
            if resp.status_code == 200:
                return True
            # 429 = rate limit: 텔레그램이 알려준 대기시간만큼 쉬고 재시도
            if resp.status_code == 429:
                retry_after = 3
                try:
                    retry_after = resp.json().get("parameters", {}).get("retry_after", 3)
                except Exception:
                    pass
                print(f"[telegram] rate limit, {retry_after}s 대기 후 재시도 ({attempt+1}/{retries})")
                time.sleep(retry_after + 1)
                continue
            # 그 외 오류(400 등): 내용 로그 남기고 중단 (재시도 무의미)
            print(f"[telegram] 발송 실패 status={resp.status_code}: {resp.text[:200]}")
            return False
        except Exception as e:
            print(f"[telegram] error ({attempt+1}/{retries}): {e}")
            time.sleep(2)
    print("[telegram] 최종 발송 실패")
    return False


# ============================================================
# 데이터 소스: GeckoTerminal
# ============================================================
def gecko_get(path, params=None):
    try:
        r = requests.get(f"{GECKO_BASE}/{path}", params=params, headers=UA, timeout=15)
        if r.status_code == 200:
            return r.json()
    except Exception as e:
        print(f"[gecko] {path} error: {e}")
    return None


def collect_candidate_mints():
    """트렌딩 풀 + 신규 풀 + DexScreener 부스트에서 후보 mint 수집.
    반환: (mints, boosted, source_counts) — source_counts로 어느 소스가 비는지 추적."""
    mints = set()
    src = {"gecko_trend": 0, "gecko_new": 0, "dex_boost": 0}

    # 1) GeckoTerminal 트렌딩/신규 풀
    for path, key in [("networks/solana/trending_pools", "gecko_trend"),
                      ("networks/solana/new_pools", "gecko_new")]:
        data = gecko_get(path, {"page": 1})
        if not data:
            continue
        for pool in data.get("data", []):
            try:
                token_id = pool["relationships"]["base_token"]["data"]["id"]
                # 형식: "solana_<mint주소>"
                if token_id.startswith("solana_"):
                    mints.add(token_id.split("_", 1)[1])
                    src[key] += 1
            except (KeyError, TypeError):
                continue

    # 2) DexScreener 부스트 (유료 홍보 중인 토큰 = 마케팅 자금 투입 신호)
    boosted = set()
    for path in ["/token-boosts/latest/v1", "/token-boosts/top/v1"]:
        try:
            r = requests.get(f"{DEXS_BASE}{path}", headers=UA, timeout=15)
            if r.status_code == 200:
                for item in r.json():
                    if item.get("chainId") == "solana":
                        addr = item.get("tokenAddress")
                        if addr:
                            mints.add(addr)
                            boosted.add(addr)
                            src["dex_boost"] += 1
        except Exception as e:
            print(f"[dexscreener boosts] error: {e}")

    return mints, boosted, src


# ============================================================
# 데이터 소스: DexScreener 페어 상세
# ============================================================
def fetch_pairs(mints):
    """mint 목록 → 각 토큰의 최대 유동성 솔라나 페어 반환 (30개씩 배치)."""
    best = {}
    mints = list(mints)
    for i in range(0, len(mints), 30):
        chunk = ",".join(mints[i : i + 30])
        try:
            r = requests.get(f"{DEXS_BASE}/latest/dex/tokens/{chunk}", headers=UA, timeout=20)
            if r.status_code != 200:
                continue
            for pair in (r.json().get("pairs") or []):
                if pair.get("chainId") != "solana":
                    continue
                mint = (pair.get("baseToken") or {}).get("address")
                if not mint:
                    continue
                liq = ((pair.get("liquidity") or {}).get("usd")) or 0
                if mint not in best or liq > (((best[mint].get("liquidity") or {}).get("usd")) or 0):
                    best[mint] = pair
        except Exception as e:
            print(f"[dexscreener pairs] error: {e}")
        time.sleep(0.3)
    return best


# ============================================================
# EMA 크로스 (GeckoTerminal 5분봉)
# ============================================================
def ema_series(closes, n):
    k = 2 / (n + 1)
    e = closes[0]
    out = [e]
    for c in closes[1:]:
        e = c * k + e * (1 - k)
        out.append(e)
    return out


def ema_signal(pool_address):
    """5분봉 EMA9/EMA21. 반환: 'golden'(직전 크로스) / 'above'(정배열) / None"""
    data = gecko_get(
        f"networks/solana/pools/{pool_address}/ohlcv/minute",
        {"aggregate": 5, "limit": 60},
    )
    try:
        ohlcv = data["data"]["attributes"]["ohlcv_list"]  # [ts, o, h, l, c, v]
        ohlcv = sorted(ohlcv, key=lambda x: x[0])  # 시간 오름차순 보장
        closes = [float(c[4]) for c in ohlcv if c[4] is not None]
        if len(closes) < 25:
            return None
        e9, e21 = ema_series(closes, 9), ema_series(closes, 21)
        if e9[-1] > e21[-1]:
            # 최근 3봉 이내 크로스 발생이면 golden
            for j in range(-3, 0):
                if e9[j - 1] <= e21[j - 1] and e9[j] > e21[j]:
                    return "golden"
            return "above"
    except (KeyError, TypeError, IndexError):
        pass
    return None


# ============================================================
# RugCheck 필터
# ============================================================
def rugcheck(mint):
    """RugCheck full report 조회.
    반환: (dangers리스트, warn_count, top_holder_pct). 실패 시 (None, None, None).
    top_holder_pct: 상위 홀더 집중도(%) 또는 None."""
    try:
        r = requests.get(f"{RUG_BASE}/tokens/{mint}/report", headers=UA, timeout=15)
        if r.status_code != 200:
            # full report 실패 시 summary로 폴백 (홀더 정보는 못 얻음)
            r2 = requests.get(f"{RUG_BASE}/tokens/{mint}/report/summary", headers=UA, timeout=15)
            if r2.status_code != 200:
                return None, None, None
            j = r2.json()
            risks = j.get("risks") or []
            dangers = [x.get("name", "?") for x in risks if x.get("level") == "danger"]
            warns = sum(1 for x in risks if x.get("level") == "warn")
            return dangers, warns, None

        j = r.json()
        risks = j.get("risks") or []
        dangers = [x.get("name", "?") for x in risks if x.get("level") == "danger"]
        warns = sum(1 for x in risks if x.get("level") == "warn")

        # 상위 홀더 집중도 계산: topHolders의 pct 합산 (인사이더/LP 제외한 순수 보유자)
        top_pct = None
        holders = j.get("topHolders") or []
        if holders:
            # 각 홀더의 pct 필드 합산 (상위 10개까지)
            total = 0.0
            for h in holders[:10]:
                p = h.get("pct")
                if p is not None:
                    total += float(p)
            if total > 0:
                top_pct = round(total, 1)

        return dangers, warns, top_pct
    except Exception as e:
        print(f"[rugcheck] {mint} error: {e}")
        return None, None, None


# ============================================================
# Reddit 언급량
# ============================================================
def scan_reddit(symbols):
    """서브레딧 신규글에서 '$SYMBOL' 패턴 언급 횟수 집계 → Redis 시간버킷 저장.
    반환: {symbol: (이번시간 카운트, 직전시간 카운트)}"""
    counts = {s: 0 for s in symbols}
    for sub in SUBREDDITS:
        try:
            r = requests.get(
                f"https://www.reddit.com/r/{sub}/new.json?limit=100",
                headers=UA, timeout=15,
            )
            if r.status_code != 200:
                continue
            for post in r.json()["data"]["children"]:
                d = post["data"]
                text = f"{d.get('title','')} {d.get('selftext','')}".upper()
                for sym in symbols:
                    # '$SYM' 정확 매칭만 (SOL 같은 짧은 심볼 오탐 방지)
                    if f"${sym.upper()}" in text:
                        counts[sym] += 1
        except Exception as e:
            print(f"[reddit] r/{sub} error: {e}")
        time.sleep(1)

    hour_now = int(time.time() // 3600)
    result = {}
    for sym, c in counts.items():
        if c > 0:
            r_cmd("SETEX", f"reddit:{sym}:{hour_now}", 7200, c)
        prev = r_cmd("GET", f"reddit:{sym}:{hour_now - 1}")
        result[sym] = (c, int(prev) if prev else 0)
    return result


# ============================================================
# 스코어링 (0~100)
# ============================================================
def score_pair(pair, boosted, ema_flag=None, reddit_delta=0):
    s = 0.0
    reasons = []

    vol = pair.get("volume") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}
    pc = pair.get("priceChange") or {}

    v1 = float(vol.get("h1") or 0)
    v24 = float(vol.get("h24") or 0)

    # 1) 거래량 급증: h1이 24h 시간평균 대비 몇 배인가 (최대 30점, 5배=만점)
    if v24 > 0:
        ratio = v1 / (v24 / 24)
        pts = min(30, ratio * 6)
        s += pts
        if ratio >= 3:
            reasons.append(f"거래량 {ratio:.1f}x 급증")

    # 2) 매수 압력 (최대 15점)
    buys, sells = int(txns.get("buys") or 0), int(txns.get("sells") or 0)
    if buys + sells >= 30:
        br = buys / (buys + sells)
        s += max(0, (br - 0.5) * 100) * 0.3  # br=1.0 → 15점
        if br >= 0.65:
            reasons.append(f"매수비율 {br*100:.0f}%")

    # 3) 가격 모멘텀 (h1 최대 20점, m5 최대 10점)
    pc_h1 = float(pc.get("h1") or 0)
    pc_m5 = float(pc.get("m5") or 0)
    if pc_h1 > 0:
        s += min(20, pc_h1 / 5)  # +100% → 20점
    if pc_m5 > 0:
        s += min(10, pc_m5)

    # 4) DexScreener 부스트 (5점)
    mint = (pair.get("baseToken") or {}).get("address")
    if mint in boosted:
        s += 5
        reasons.append("DexScreener 부스트 중")

    # 5) 신규 상장 프리미엄 48h 이내 (5점)
    created = pair.get("pairCreatedAt")
    if created and (time.time() * 1000 - created) < 48 * 3600 * 1000:
        s += 5

    # 6) EMA 크로스 (골든크로스 10점 / 정배열 5점)
    if ema_flag == "golden":
        s += 10
        reasons.append("5분봉 EMA9/21 골든크로스")
    elif ema_flag == "above":
        s += 5

    # 7) 레딧 언급 증가 (최대 10점)
    if reddit_delta > 0:
        s += min(10, reddit_delta * 3)
        reasons.append(f"레딧 언급 +{reddit_delta}건/h 증가")

    return round(s, 1), reasons


def passes_filters(pair):
    liq = ((pair.get("liquidity") or {}).get("usd")) or 0
    v24 = float((pair.get("volume") or {}).get("h24") or 0)
    created = pair.get("pairCreatedAt") or 0
    age_min = (time.time() * 1000 - created) / 60000 if created else 0
    if liq < MIN_LIQ_USD:
        return False
    if v24 < MIN_VOL24_USD:
        return False
    if created and age_min < MIN_AGE_MIN:
        return False
    if created and age_min > MAX_AGE_DAYS * 1440:
        return False
    return True


def passes_shadow_filters(pair):
    """섀도우 추적 대상 판정: 나이만 20분 미만, 나머지(유동성/거래량)는 정식과 동일 기준.
    → '20분 미만이라 위험하다'는 가정 자체를 나이 조건만 바꿔 비교 검증."""
    liq = ((pair.get("liquidity") or {}).get("usd")) or 0
    v24 = float((pair.get("volume") or {}).get("h24") or 0)
    created = pair.get("pairCreatedAt") or 0
    age_min = (time.time() * 1000 - created) / 60000 if created else 0
    if liq < MIN_LIQ_USD or v24 < MIN_VOL24_USD:
        return False
    if not created or age_min >= MIN_AGE_MIN:
        return False
    return True


def shadow_tick(pairs):
    """20분 미만 코인을 알림 없이 조용히 perf 추적에 등록 (나이필터 근거 검증용)."""
    if not (SHADOW_ENABLE and PERF_ENABLE):
        return
    for mint, pair in pairs.items():
        if not passes_shadow_filters(pair):
            continue
        if r_cmd("GET", f"shadowed:{mint}"):
            continue
        r_cmd("SETEX", f"shadowed:{mint}", SHADOW_TTL, "1")
        sym = (pair.get("baseToken") or {}).get("symbol", "?")
        price = float(pair.get("priceUsd") or 0)
        perf_add(mint, sym, "shadow", price)
        print(f"[SHADOW] {sym} 추적시작 (20분 미만, 알림없음)")


def doubledip_tick(pairs):
    """2단 눌림(더블딥) 관찰 트래커 — 알림 없이 상태만 추적, 검증목적.
    상태(코인별 doubledip:{mint}): {stage, swing_high, bounce_high, d1_ts}
      stage 0(암묵) → 고점 갱신만
      stage 1: 1차 눌림(-10%) 감지 → swing_high 기록, 반등 대기
      stage 2: 반등해서 bounce_high 갱신 중
      확정: bounce_high 대비 -30% 눌림이 1차 눌림 후 1시간 이내 발생 → perf 등록(kind='doubledip')
    ★ Redis 절감: 대상 코인 상태를 파이프라인으로 일괄 조회."""
    if not (DOUBLEDIP_ENABLE and PERF_ENABLE):
        return
    cands = []
    for mint, pair in pairs.items():
        mc = float(pair.get("marketCap") or pair.get("fdv") or 0)
        price = float(pair.get("priceUsd") or 0)
        if mc < DOUBLEDIP_MC_MIN or price <= 0:
            continue
        if not passes_filters(pair) and not passes_shadow_filters(pair):
            continue  # 유동성/거래량 기본 품질 기준은 유지 (나이만 무관)
        cands.append((mint, pair, price))
    if not cands:
        return

    raws = r_pipe([["GET", f"doubledip:{m}"] for m, _, _ in cands])
    now = time.time()
    writes = []
    for (mint, pair, price), raw in zip(cands, raws):
        sym = (pair.get("baseToken") or {}).get("symbol", "?")
        try:
            st = json.loads(raw) if raw else None
        except Exception:
            st = None

        if st is None:
            # 최초 관찰: 고점 추적 시작
            writes.append(["SETEX", f"doubledip:{mint}", DOUBLEDIP_STATE_TTL,
                           json.dumps({"stage": 0, "swing_high": price, "bounce_high": None, "d1_ts": None})])
            continue

        stage = st.get("stage", 0)

        if stage == 0:
            swing_high = st.get("swing_high", price)
            if price > swing_high:
                st["swing_high"] = price
            else:
                drop = (swing_high - price) / swing_high * 100
                if drop >= DOUBLEDIP_D1_PCT:
                    # 1차 눌림 확정 → stage 1 (반등 대기)
                    st = {"stage": 1, "swing_high": swing_high, "bounce_high": price, "d1_ts": now}
            writes.append(["SETEX", f"doubledip:{mint}", DOUBLEDIP_STATE_TTL, json.dumps(st)])
            continue

        if stage == 1:
            # 시간 초과면 리셋 (1차→2차 윈도우 밖)
            if now - (st.get("d1_ts") or 0) > DOUBLEDIP_WINDOW_S:
                writes.append(["SETEX", f"doubledip:{mint}", DOUBLEDIP_STATE_TTL,
                               json.dumps({"stage": 0, "swing_high": price, "bounce_high": None, "d1_ts": None})])
                continue
            bounce_high = st.get("bounce_high", price)
            if price > bounce_high:
                # 반등 고점 계속 갱신
                st["bounce_high"] = price
                writes.append(["SETEX", f"doubledip:{mint}", DOUBLEDIP_STATE_TTL, json.dumps(st)])
                continue
            drop2 = (bounce_high - price) / bounce_high * 100
            if drop2 >= DOUBLEDIP_D2_PCT:
                # ★ 2단 눌림 확정 — 알림 없이 관찰 등록만
                perf_add(mint, sym, "doubledip", price)
                print(f"[DOUBLEDIP] {sym} 2단눌림 확정 (1차 -{DOUBLEDIP_D1_PCT}%→반등→2차 -{drop2:.0f}%) 관찰등록, 알림없음")
                writes.append(["DEL", f"doubledip:{mint}"])  # 확정 후 상태 종료(재확정 방지)
            else:
                writes.append(["SETEX", f"doubledip:{mint}", DOUBLEDIP_STATE_TTL, json.dumps(st)])

    if writes:
        r_pipe(writes)


# ============================================================
# 눌림목(DIP) 감지
# ============================================================
def check_dip(pair, entry_h1=None):
    """상승추세 살아있는데 단기 눌림 중인지 판정.
    entry_h1: 급등 알림 등록 당시의 1h 상승률 (추세붕괴 판정용).
    반환: (is_dip, reasons, is_veteran). is_veteran: 오래된 검증 코인 여부."""
    if not DIP_ENABLE:
        return False, [], False

    pc = pair.get("priceChange") or {}
    vol = pair.get("volume") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}
    pc_h1 = float(pc.get("h1") or 0)
    pc_h24 = float(pc.get("h24") or 0)
    pc_m5 = float(pc.get("m5") or 0)
    v1 = float(vol.get("h1") or 0)
    v24 = float(vol.get("h24") or 0)
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)

    # 코인 나이 → 오래된 코인(veteran) 판정. 다이아몬드 핸드 → 기준 완화
    created = pair.get("pairCreatedAt")
    age_days = (time.time() * 1000 - created) / 86400000 if created else 0
    is_veteran = age_days >= DIP_VETERAN_DAYS
    ease = DIP_VETERAN_EASE if is_veteran else 1.0

    reasons = []

    # 조건1: 큰 추세 살아있음 (1h 또는 24h가 충분히 상승)
    if not (pc_h1 >= DIP_TREND_H1 or pc_h24 >= DIP_TREND_H1 * 2):
        return False, [], False

    # 조건2: 단기(5m)로만 눌림 — 얕지도 깊지도 않게
    if not (DIP_M5_MIN <= pc_m5 <= DIP_M5_MAX):
        return False, [], False

    # 조건3: 눌리는 중에도 매수세 우세 (오래된 코인은 기준 완화)
    if buys + sells >= 30:
        buy_ratio = buys / (buys + sells)
        if buy_ratio < DIP_BUY_RATIO * ease:
            return False, [], False
        reasons.append(f"매수비율 {buy_ratio*100:.0f}%")

    # 조건4: 눌리는 중에도 거래량 유지 (오래된 코인은 기준 완화)
    if v24 > 0:
        vol_ratio = v1 / (v24 / 24)
        if vol_ratio < DIP_VOL_RATIO * ease:
            return False, [], False
        reasons.append(f"거래량 {vol_ratio:.1f}x 유지")

    # 조건5: 추세붕괴 방지 — 등록시점보다 크게 꺾였고 절대수치도 낮으면 제외
    if entry_h1 and entry_h1 > 0:
        faded = pc_h1 < entry_h1 * DIP_MAX_FADE
        below_floor = pc_h1 < DIP_TREND_H1
        if faded and below_floor:
            return False, [], False

    reasons.insert(0, f"추세 1h +{pc_h1:.0f}% 중 5m {pc_m5:.1f}% 눌림")
    if is_veteran:
        reasons.append(f"🏆 검증된 코인 ({age_days:.0f}일 생존)")
    return True, reasons, is_veteran


def send_dip_alert(pair, reasons, warns, top_holder_pct=None, detect_ts=None):
    base = pair.get("baseToken") or {}
    sym = base.get("symbol", "?")
    name = base.get("name", "?")
    mint = base.get("address", "")
    price = pair.get("priceUsd", "?")
    liq = fmt_usd((pair.get("liquidity") or {}).get("usd"))
    v1 = fmt_usd((pair.get("volume") or {}).get("h1"))
    pc = pair.get("priceChange") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}

    reason_line = " / ".join(reasons) if reasons else "-"
    verdict, heat_line, holder_line = build_insights(pair, top_holder_pct)
    tag_verdict, pos_tags, cau_tags = build_tags(pair, top_holder_pct)

    # ── 1초 판단용 압축 위험요약 ──
    risk_bits = []
    if warns:
        risk_bits.append(f"Rug⚠️{warns}")
    if heat_line and "🔥" in heat_line:
        risk_bits.append("과열🔥")
    for t in cau_tags:
        if "봇펌핑" in t:
            risk_bits.append("봇펌핑의심⚠️")
        elif "🚫" in t:
            risk_bits.append("홀더초집중🚫")
        elif "🔓" in t:
            risk_bits.append("홀더집중🔓")
        elif "🔬" in t:
            risk_bits.append("초저시총🔬")
        elif "🐋" in t:
            risk_bits.append("고시총🐋")
    risk_line = ("⚠️ " + " · ".join(risk_bits)) if risk_bits else "🟢 주요 위험 신호 없음"

    header = (
        f"🎯 <b>{name} (${sym})</b> 눌림목\n"
        f"{{DELAY}} · 5m {pc.get('m5','?')}% · 1h {pc.get('h1','?')}%↑ 중 눌림\n"
        f"{risk_line}"
    )

    detail = (
        f"💰 ${price} · 💧{liq} · 거래량 {v1}\n"
        f"📊 매수/매도 {txns.get('buys','?')}/{txns.get('sells','?')} · 🎯{reason_line}"
    )

    msg = (
        f"{header}\n{detail}\n"
        f"<code>{mint}</code>\n"
        f'<a href="https://dexscreener.com/solana/{pair.get("pairAddress","")}">차트</a> · '
        f'<a href="https://rugcheck.xyz/tokens/{mint}">Rug</a>'
    )
    return tg_send(msg, detect_ts=detect_ts, buttons=[(f"📌 진입 등록 (현재가·손절-{SELL_SL_PCT:g}%)", f"reg:{mint}")])


# ============================================================
# 알림 메시지
# ============================================================
def fmt_usd(x):
    x = float(x or 0)
    if x >= 1_000_000:
        return f"${x/1_000_000:.2f}M"
    if x >= 1_000:
        return f"${x/1_000:.1f}K"
    return f"${x:.0f}"


def buy_buttons(mint):
    """금액별 원클릭 매수 링크 생성.
    jup.ag 스왑 페이지를 SOL→해당토큰, 금액 프리필 시도하며 연다.
    프리필이 플랫폼에 따라 무시되면 팬텀에서 금액만 입력하면 됨."""
    links = []
    for amt in BUY_AMOUNTS:
        # inputAmount 파라미터: 지원되면 금액 자동 입력, 아니면 페이지만 열림(무해)
        url = f"https://jup.ag/swap/SOL-{mint}?inputMint={SOL_MINT}&inputAmount={amt}"
        links.append(f'<a href="{url}">💸 {amt} SOL 매수</a>')
    return "  ·  ".join(links)


def build_insights(pair, top_holder_pct=None):
    """알림에 넣을 인사이트 3종 계산.
    반환: (risk_verdict, heat_line, holder_line) — 각각 문자열 또는 None."""
    pc = pair.get("priceChange") or {}
    vol = pair.get("volume") or {}
    liq = float((pair.get("liquidity") or {}).get("usd") or 0)
    v24 = float(vol.get("h24") or 0)
    v1 = float(vol.get("h1") or 0)
    pc_h24 = float(pc.get("h24") or 0)
    pc_h1 = float(pc.get("h1") or 0)
    created = pair.get("pairCreatedAt")
    age_h = (time.time() * 1000 - created) / 3600000 if created else None

    # --- 1) 진입 위험도 한줄 판정 ---
    # 여러 신호를 종합해 추격/초기/과열 등을 판정
    flags = []
    if pc_h24 >= 300:
        flags.append("이미 큰 상승")
    if age_h is not None and age_h < 3:
        flags.append("초기 국면")
    if age_h is not None and age_h > 24:
        flags.append("성숙 단계")
    # 시간당 상승 속도 (나이 대비)
    if age_h and age_h > 0 and pc_h24 > 0:
        per_h = pc_h24 / min(age_h, 24)
        if per_h > 80:
            flags.append("급가속")

    if pc_h24 >= 500 or (pc_h1 >= 150):
        verdict = "🔴 추격 위험 — 고점 근처 가능, 눌림 대기 권장"
    elif "초기 국면" in flags:
        verdict = "🟡 초기 국면 — 변동성 큼, 소액 테스트"
    elif pc_h1 > 0 and pc_h24 > 0:
        verdict = "🟢 상승 진행 — 흐름 확인 후 분할"
    else:
        verdict = "⚪ 혼조 — 신중"
    if flags:
        verdict += f"  ({', '.join(flags)})"

    # --- 2) 거래량/유동성 과열도 ---
    heat_line = None
    if liq > 0 and v1 > 0:
        heat_ratio = v1 / liq  # 1시간 거래량이 유동성의 몇 배
        if heat_ratio >= 5:
            heat_line = f"🔥 과열 (1h 거래량 = 유동성 {heat_ratio:.1f}배) — 급락 리스크"
        elif heat_ratio >= 2:
            heat_line = f"♨️ 활발 (1h 거래량 = 유동성 {heat_ratio:.1f}배)"
        else:
            heat_line = f"💧 안정 (1h 거래량 = 유동성 {heat_ratio:.1f}배)"

    # --- 3) 홀더 집중도 ---
    holder_line = None
    if top_holder_pct is not None:
        if top_holder_pct >= 50:
            holder_line = f"🔒 상위홀더 {top_holder_pct:.0f}% — 매우 집중, 덤핑 위험 큼"
        elif top_holder_pct >= 30:
            holder_line = f"🔓 상위홀더 {top_holder_pct:.0f}% — 다소 집중, 주의"
        else:
            holder_line = f"✅ 상위홀더 {top_holder_pct:.0f}% — 분산 양호"

    return verdict, heat_line, holder_line


def build_tags(pair, top_holder_pct=None):
    """전문가 기준 태그 생성 + 종합 판정.
    반환: (verdict_line, positive_tags, caution_tags)
      verdict_line: 종합 등급 한 줄 (예: '👍 양호 (긍정 3 · 주의 0)')
      positive_tags/caution_tags: 각각 좋은/주의 태그 리스트"""
    if not TAG_ENABLE:
        return None, [], []

    pos = []   # 긍정 신호
    cau = []   # 주의 신호
    pc = pair.get("priceChange") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}
    pc_h1 = float(pc.get("h1") or 0)
    buys = int(txns.get("buys") or 0)
    sells = int(txns.get("sells") or 0)
    tx_total = buys + sells
    created = pair.get("pairCreatedAt")
    age_min = (time.time() * 1000 - created) / 60000 if created else None

    mc = float(pair.get("marketCap") or pair.get("fdv") or 0)

    # 1) 시총 (스위트스팟=긍정, 극단=주의)
    if mc > 0:
        mc_txt = f"${mc/1_000_000:.1f}M" if mc >= 1_000_000 else f"${mc/1000:.0f}K"
        if TAG_MC_CORE_LOW <= mc <= TAG_MC_CORE_HIGH:
            pos.append(f"⭐ 시총 스위트스팟 ({mc_txt}, 1~5M 최적)")
        elif TAG_MC_EXT_LOW <= mc <= TAG_MC_EXT_HIGH:
            pos.append(f"✨ 시총 진입가능 ({mc_txt}, 0.5~10M)")
        elif mc < TAG_MC_EXT_LOW:
            cau.append(f"🔬 시총 너무 낮음 ({mc_txt}, <0.5M 고위험)")
        else:
            cau.append(f"🐋 시총 높음 ({mc_txt}, >10M 배수제한)")

    # 2) 거래 건수 (250+ = 긍정, 미달 = 중립이라 표시 안 함)
    if tx_total >= TAG_MIN_TXNS:
        pos.append(f"🔁 거래 활발 (1h {tx_total}건, 250+ 기준)")

    # 3) 홀더 집중도
    if top_holder_pct is not None:
        if top_holder_pct >= TAG_INSIDER_DANGER:
            cau.append(f"🚫 홀더 초집중 ({top_holder_pct:.0f}%, 50%+ 사기수준)")
        elif top_holder_pct >= TAG_INSIDER_WARN:
            cau.append(f"🔓 홀더 다소집중 ({top_holder_pct:.0f}%, 20%+ 주의)")
        else:
            pos.append(f"👥 홀더 분산양호 ({top_holder_pct:.0f}%, 20%↓)")

    # 4) 봇펌핑 의심 (어린데 급등 = 주의)
    if age_min is not None and age_min < TAG_BOT_AGE_MIN and pc_h1 >= TAG_BOT_PUMP_H1:
        cau.append(f"⚠️ 봇펌핑 의심 ({age_min:.0f}분만에 +{pc_h1:.0f}%)")

    # 종합 판정: 긍정 개수 vs 주의 개수
    np, nc = len(pos), len(cau)
    if np + nc == 0:
        verdict = None
    elif nc == 0 and np >= 2:
        verdict = f"👍 양호 (긍정 {np} · 주의 0)"
    elif nc == 0 and np >= 1:
        verdict = f"🙂 무난 (긍정 {np} · 주의 0)"
    elif nc >= 2 or (nc >= 1 and any("사기수준" in c or "봇펌핑" in c for c in cau)):
        verdict = f"👎 주의 (긍정 {np} · 주의 {nc})"
    else:
        verdict = f"😐 혼조 (긍정 {np} · 주의 {nc})"

    return verdict, pos, cau


def entry_grade(pair, score, top_holder_pct=None):
    """진입등급 S/A/B/C 계산 — 점수(모멘텀) × 종합판정(구조) × 과열도.
    반환: (등급, 한줄가이드)."""
    verdict_line, _, _ = build_tags(pair, top_holder_pct)
    v = verdict_line or ""
    _, heat_line, _ = build_insights(pair, top_holder_pct)
    overheated = bool(heat_line and "🔥" in heat_line)

    if "👎" in v:
        return "C", "관망 권장 — 구조 리스크"
    if "😐" in v or overheated:
        why = "과열" if overheated else "구조 혼조"
        return "B", f"소액만 — {why}"
    good = ("👍" in v) or ("🙂" in v)
    if score >= 75 and "👍" in v and not overheated:
        return "S", "적극 검토 — 모멘텀·구조 모두 강함"
    if good:
        return "A", "표준 진입"
    return "B", "소액만 — 판정 정보 부족"


def send_alert(pair, score, reasons, dangers, warns, top_holder_pct=None, detect_ts=None):
    base = pair.get("baseToken") or {}
    sym = base.get("symbol", "?")
    name = base.get("name", "?")
    mint = base.get("address", "")
    price = pair.get("priceUsd", "?")
    liq = fmt_usd((pair.get("liquidity") or {}).get("usd"))
    v1 = fmt_usd((pair.get("volume") or {}).get("h1"))
    v24 = fmt_usd((pair.get("volume") or {}).get("h24"))
    pc = pair.get("priceChange") or {}
    txns = (pair.get("txns") or {}).get("h1") or {}
    created = pair.get("pairCreatedAt")
    age = ""
    if created:
        h = (time.time() * 1000 - created) / 3600000
        age = f"{h/24:.1f}일" if h >= 48 else f"{h:.1f}시간"

    reason_line = " / ".join(reasons) if reasons else "-"
    verdict, heat_line, holder_line = build_insights(pair, top_holder_pct)
    tag_verdict, pos_tags, cau_tags = build_tags(pair, top_holder_pct)
    grade, guide = entry_grade(pair, score, top_holder_pct)

    # ── 1초 판단용 압축 위험요약 (절대 숨기지 않음) ──
    grade_dot = {"S": "🟢", "A": "🟢", "B": "🟡", "C": "🔴"}.get(grade, "⚪")
    risk_bits = []
    if warns:
        risk_bits.append(f"Rug⚠️{warns}")
    if heat_line and "🔥" in heat_line:
        risk_bits.append("과열🔥")
    for t in cau_tags:
        if "봇펌핑" in t:
            risk_bits.append("봇펌핑의심⚠️")
        elif "🚫" in t:
            risk_bits.append("홀더초집중🚫")
        elif "🔓" in t:
            risk_bits.append("홀더집중🔓")
        elif "🔬" in t:
            risk_bits.append("초저시총🔬")
        elif "🐋" in t:
            risk_bits.append("고시총🐋")
    risk_line = ("⚠️ " + " · ".join(risk_bits)) if risk_bits else "🟢 주요 위험 신호 없음"

    header = (
        f"🚨 <b>{name} (${sym})</b>  {grade_dot}<b>{grade}</b>·{score}점\n"
        f"{{DELAY}} · 1h {pc.get('h1','?')}%↑ · 24h {pc.get('h24','?')}%↑ · 나이 {age}\n"
        f"{risk_line}\n"
        f"💡 {guide}"
    )

    # 핵심 수치 2줄만 (스포일러 미지원 클라이언트 대응 — 짧게 쳐냄)
    detail = (
        f"💰 ${price} · 💧{liq} · 거래량 {v1}\n"
        f"📊 5m {pc.get('m5','?')}% · 매수/매도 {txns.get('buys','?')}/{txns.get('sells','?')} · 🎯{reason_line}"
    )

    msg = (
        f"{header}\n{detail}\n"
        f"<code>{mint}</code>\n"
        f'<a href="https://dexscreener.com/solana/{pair.get("pairAddress","")}">차트</a> · '
        f'<a href="https://rugcheck.xyz/tokens/{mint}">Rug</a>'
    )
    return tg_send(msg, detect_ts=detect_ts, buttons=[(f"📌 진입 등록 (현재가·손절-{SELL_SL_PCT:g}%)", f"reg:{mint}")])


# ============================================================
# 메인 루프
# ============================================================
def main():
    # 가동 메시지는 하루 1회만 (무료 티어 슬립/재시작 반복 시 스팸 방지)
    if not r_cmd("GET", "boot_notified"):
        tg_send(
            "🤖 <b>Solana Meme Screener v1.0 가동</b>\n"
            f"스캔주기 {LOOP_SEC}s · 알림점수 ≥{ALERT_SCORE} · 최소유동성 {fmt_usd(MIN_LIQ_USD)}\n"
            "소스: DexScreener + GeckoTerminal + RugCheck + Reddit"
        )
        r_cmd("SETEX", "boot_notified", 86400, "1")  # 24시간 후 만료

    # 텔레그램 명령 폴러 시작 (중복 방지 가드 — 워치독 재시작에도 1개만)
    global _POLLER_STARTED
    if not _POLLER_STARTED:
        threading.Thread(target=tg_poll_loop, daemon=True, name="tgpoller").start()
        _POLLER_STARTED = True

    cycle = 0
    reddit_cache = {}  # {symbol: delta}

    while True:
        cycle += 1
        try:
            mints, boosted, src = collect_candidate_mints()
            n = len(mints)
            print(f"[cycle {cycle}] candidates: {n} (trend {src['gecko_trend']} / new {src['gecko_new']} / boost {src['dex_boost']})")

            # 포지션·성과추적 코인은 트렌딩에서 빠져도 항상 시세 확보 (감시 필수)
            for pm in (r_cmd("SMEMBERS", "pos:index") or []):
                mints.add(pm)
            if PERF_ENABLE:
                for pk in (r_cmd("SMEMBERS", "perf:index") or []):
                    seg = pk.split(":")
                    if len(seg) >= 3:
                        mints.add(seg[2])

            pairs = fetch_pairs(mints)

            # 상태 기록 (브라우저 /status 로 확인용)
            r_cmd("SET", "status:cycle", cycle)
            r_cmd("SET", "status:candidates", n)
            r_cmd("SET", "status:last_scan", int(time.time()))
            # 흐름 이력: "후보수:trend:new:boost" 를 리스트 앞에 넣고 최근 20개만 유지
            hist = f"{n}:{src['gecko_trend']}:{src['gecko_new']}:{src['dex_boost']}"
            r_cmd("LPUSH", "status:history", hist)
            r_cmd("LTRIM", "status:history", 0, 19)

            # 1차 필터 + 기본 점수
            scored = []
            for mint, pair in pairs.items():
                if not passes_filters(pair):
                    continue
                base_score, _ = score_pair(pair, boosted)
                scored.append((base_score, mint, pair))
            scored.sort(reverse=True, key=lambda x: x[0])

            # ★ 쿨다운 걸린 코인을 '슬롯 자르기 전에' 미리 제거 (핵심 수정)
            # 안 그러면 쿨다운 걸린 상위 코인들이 정밀검사 슬롯(OHLCV_TOP_N)을
            # 다 차지해서, 그 아래의 새 급등 코인이 검사조차 못 받음.
            fresh = []
            skipped_cd = 0
            for item in scored:
                if r_cmd("GET", f"alerted:{item[1]}"):
                    skipped_cd += 1
                    continue
                fresh.append(item)
            if skipped_cd:
                print(f"[cycle {cycle}] 쿨다운 스킵 {skipped_cd}개 → 새 후보 {len(fresh)}개 정밀검사 대상")
            r_cmd("SET", "status:cooldown_skip", skipped_cd)
            r_cmd("SET", "status:fresh_count", len(fresh))

            # 레딧 스캔 (N사이클마다, 상위 '새' 후보 심볼만)
            if cycle % REDDIT_CYCLES == 1 and fresh:
                syms = list({
                    (p.get("baseToken") or {}).get("symbol", "")
                    for _, _, p in fresh[:20]
                } - {""})
                rd = scan_reddit(syms)
                reddit_cache = {s: max(0, cur - prev) for s, (cur, prev) in rd.items()}

            # 쿨다운 제거된 새 후보 중 상위만 EMA + RugCheck 정밀 검사
            for base_score, mint, pair in fresh[:OHLCV_TOP_N]:
                sym = (pair.get("baseToken") or {}).get("symbol", "")
                ema_flag = ema_signal(pair.get("pairAddress", ""))
                rdelta = reddit_cache.get(sym, 0)
                final_score, reasons = score_pair(pair, boosted, ema_flag, rdelta)

                if final_score < ALERT_SCORE:
                    continue

                # RugCheck: danger 있으면 알림 자체를 스킵
                dangers, warns, top_pct = rugcheck(mint)
                if dangers:
                    print(f"[skip-rug] {sym} {mint}: {dangers}")
                    r_cmd("SETEX", f"alerted:{mint}", COOLDOWN_SEC, "rug")
                    continue

                detect_ts = time.time()  # 발송 확정 시각 (지연 배지 기준점)
                sent_ok = send_alert(pair, final_score, reasons, dangers, warns or 0, top_pct, detect_ts)
                # 쿨다운은 발송 성공/실패 무관하게 설정 (실패해도 다음 사이클 폭주 방지)
                r_cmd("SETEX", f"alerted:{mint}", COOLDOWN_SEC, str(final_score))
                # 카운터/마지막알림은 실제 발송 성공했을 때만 기록 (status 정확성)
                if sent_ok:
                    r_cmd("INCR", "status:alert_count")
                    r_cmd("SET", "status:last_alert", f"🚨{sym} ({final_score}) @ {datetime.now(timezone.utc).strftime('%m-%d %H:%M')}UTC")
                    tv, _, _ = build_tags(pair, top_pct)
                    g, _ = entry_grade(pair, final_score, top_pct)
                    archive_alert("pump", sym, final_score, pair, f"[{g}] {tv or ''}")
                    perf_add(mint, sym, "pump", float(pair.get("priceUsd") or 0))
                else:
                    print(f"[ALERT-FAIL] {sym} 발송 실패 — 기록 스킵")
                # dip 추적 목록에 등록 (이 코인이 나중에 눌리면 눌림목 알림)
                # 값 = 등록 시점의 1h 상승률 (나중에 추세붕괴 판정에 사용)
                if DIP_ENABLE:
                    entry_h1 = float((pair.get("priceChange") or {}).get("h1") or 0)
                    r_cmd("SETEX", f"dip_track:{mint}", DIP_TRACK_SEC, str(entry_h1))
                print(f"[ALERT] {sym} score={final_score}")
                time.sleep(1)

            # ---- 눌림목(DIP) 스캔: 최근 급등해서 추적중인 코인 중 눌린 것 ----
            if DIP_ENABLE:
                for mint, pair in pairs.items():
                    track_val = r_cmd("GET", f"dip_track:{mint}")
                    if not track_val:
                        continue  # 급등 이력 없는 코인은 대상 아님
                    if r_cmd("GET", f"dip_alerted:{mint}"):
                        continue  # dip 알림 쿨다운 중
                    try:
                        entry_h1 = float(track_val)
                    except (ValueError, TypeError):
                        entry_h1 = None
                    is_dip, dip_reasons, is_veteran = check_dip(pair, entry_h1)
                    if not is_dip:
                        continue
                    sym = (pair.get("baseToken") or {}).get("symbol", "")
                    # B: 눌림목 레딧 언급 체크 — 언급 살아있으면 커뮤니티 활성 표시
                    if DIP_REDDIT_CHECK:
                        rmention = reddit_cache.get(sym, 0)
                        if rmention > 0:
                            dip_reasons.append(f"💬 커뮤니티 활성 (레딧 +{rmention}건)")
                    dangers, warns, top_pct = rugcheck(mint)
                    if dangers:
                        r_cmd("SETEX", f"dip_alerted:{mint}", DIP_COOLDOWN, "rug")
                        continue
                    detect_ts = time.time()
                    dip_ok = send_dip_alert(pair, dip_reasons, warns or 0, top_pct, detect_ts)
                    r_cmd("SETEX", f"dip_alerted:{mint}", DIP_COOLDOWN, "1")
                    if dip_ok:
                        # 눌림목도 status에 기록 (기존엔 급등만 기록해서 마지막알림이 안 바뀌던 버그)
                        r_cmd("INCR", "status:alert_count")
                        r_cmd("SET", "status:last_alert", f"🎯{sym} @ {datetime.now(timezone.utc).strftime('%m-%d %H:%M')}UTC")
                        tv, _, _ = build_tags(pair, top_pct)
                        archive_alert("dip", sym, "-", pair, tv)
                        perf_add(mint, sym, "dip", float(pair.get("priceUsd") or 0))
                        print(f"[DIP] {sym}")
                    else:
                        print(f"[DIP-FAIL] {sym} 발송 실패")
                    time.sleep(1)

            # ---- 포지션 매도감시 + 알림 성과추적 (매 사이클) ----
            # 포지션 감시(손절/익절)는 매 사이클 — 지연되면 안 되는 신호
            activity = market_activity(pairs)
            positions_tick(pairs, activity)
            # 성과추적/섀도우는 3사이클(약 6분)마다 — 가격 추적엔 충분, Redis 절감
            if cycle % 3 == 0:
                perf_tick(pairs)
                shadow_tick(pairs)
                doubledip_tick(pairs)

        except Exception:
            print(f"[cycle {cycle}] fatal:\n{traceback.format_exc()}")
        finally:
            # 무슨 일이 있어도(예외/빈응답 포함) 반드시 대기 → 사이클 폭주 원천 차단
            time.sleep(LOOP_SEC)


if __name__ == "__main__":
    main()
