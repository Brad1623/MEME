# -*- coding: utf-8 -*-
"""
정산 스크립트 (settle.py) — 알림 kind별 기대값 계산
====================================================
perf:done 기록(최대 200건)을 읽어서 "규칙대로 기계적으로 매매했다면"의
기대값을 kind별로 계산한다. 봇 수정 없이 단독 실행.

규칙 A (손절만): 알림가 진입, trough가 -20% 닿으면 -20%로 종료, 아니면 r24로 종료.
  → 기록만으로 100% 확정 계산 가능 (순서 모호성 없음)
규칙 B (손절+익절 ±20%): peak/trough 둘 다 ±20%에 닿은 건은
  "무엇이 먼저였는지" 기록상 알 수 없음 → 비관/낙관 범위로 표시.

실행: 봇과 같은 환경변수(UPSTASH_*)로
  python settle.py            → 콘솔 출력
  python settle.py --tg       → 텔레그램으로도 발송 (TELEGRAM_* 필요)
  python settle.py --sl 25 --tp 30  → 손절/익절 기준 변경 실험
"""
import os
import sys
import json
import argparse
from datetime import datetime, timezone

import requests

UPSTASH_URL = os.environ["UPSTASH_REDIS_REST_URL"].rstrip("/")
UPSTASH_TOKEN = os.environ["UPSTASH_REDIS_REST_TOKEN"]

SANITY_MAX_PCT = 2000.0   # 봇 PERF_SANITY_MAX_PCT와 동일 — 풀 오매칭 오염 기록 제외

KIND_LABEL = {
    "pump": "급등(정식)", "dip": "눌림목", "sustain": "지속형",
    "sustain_early": "조기지속형", "shadow": "섀도우(<20분)",
    "doubledip": "2단눌림", "cgrade": "C등급관찰",
}
# 실제 알림이 나가는 트랙 (판정 대상). 나머지는 참고용으로만 출력.
ALERT_KINDS = ["pump", "dip", "sustain", "sustain_early"]


def r_cmd(*args):
    resp = requests.post(
        UPSTASH_URL, json=list(args),
        headers={"Authorization": f"Bearer {UPSTASH_TOKEN}"}, timeout=10)
    resp.raise_for_status()
    return resp.json().get("result")


def load_done():
    raw = r_cmd("LRANGE", "perf:done", 0, 199) or []
    items = []
    for x in raw:
        try:
            it = json.loads(x)
            if isinstance(it, dict) and it.get("kind"):
                items.append(it)
        except Exception:
            pass
    # 오염 기록 제외 (봇 요약과 같은 기준)
    return [i for i in items if (i.get("peak") or 0) <= SANITY_MAX_PCT]


def settle_kind(items, sl, tp):
    """한 kind의 기록으로 규칙A/규칙B 기대값 계산.
    반환 dict — 전부 % 단위, ev_*는 1회 진입당 평균 손익."""
    # r24가 없는 기록(90000초 타임아웃 등)은 r6→r1 순으로 대체, 그것도 없으면 제외
    usable = []
    for i in items:
        final = i.get("r24")
        if final is None:
            final = i.get("r6") if i.get("r6") is not None else i.get("r1")
        if final is None:
            continue
        usable.append((float(i.get("trough") or 0), float(i.get("peak") or 0), float(final)))
    n = len(usable)
    if n == 0:
        return None

    # --- 규칙 A: 손절만 (-sl 닿으면 -sl, 아니면 final) ---
    a_res = [(-sl if tr <= -sl else fin) for tr, pk, fin in usable]
    a_ev = sum(a_res) / n
    a_stop = sum(1 for tr, _, _ in usable if tr <= -sl)

    # --- 규칙 B: 손절+익절 — 순서 모호 건은 범위로 ---
    b_pess, b_opt = [], []   # 비관(손절 우선) / 낙관(익절 우선)
    ambiguous = 0
    for tr, pk, fin in usable:
        hit_sl, hit_tp = tr <= -sl, pk >= tp
        if hit_sl and hit_tp:
            ambiguous += 1
            b_pess.append(-sl)
            b_opt.append(tp)
        elif hit_tp:
            b_pess.append(tp); b_opt.append(tp)
        elif hit_sl:
            b_pess.append(-sl); b_opt.append(-sl)
        else:
            b_pess.append(fin); b_opt.append(fin)

    return {
        "n": n,
        "a_ev": round(a_ev, 1),
        "a_stop_pct": round(a_stop / n * 100),
        "b_ev_pess": round(sum(b_pess) / n, 1),
        "b_ev_opt": round(sum(b_opt) / n, 1),
        "b_ambiguous": ambiguous,
        "tp_pct": round(sum(1 for _, pk, _ in usable if pk >= tp) / n * 100),
        "surv_pct": round(sum(1 for *_, fin in usable if fin >= 0) / n * 100),
    }


def verdict(s):
    """판정: 규칙A 기대값이 기준. 비관치까지 플러스면 KEEP 확신."""
    if s is None:
        return "표본없음"
    if s["a_ev"] > 0 and s["b_ev_pess"] > 0:
        return "✅ 유지"
    if s["a_ev"] > 0:
        return "🟡 유지(익절규칙 재검토)"
    if s["n"] < 20:
        return "⏳ 표본부족 — 관찰 지속"
    return "🔻 관찰 강등 후보"


def build_report(sl, tp):
    items = load_done()
    by_kind = {}
    for i in items:
        by_kind.setdefault(i["kind"], []).append(i)

    now = datetime.now(timezone.utc).strftime("%m-%d %H:%M UTC")
    lines = [f"📊 <b>정산 리포트</b> ({now})",
             f"규칙: 손절 -{sl:.0f}% / 익절 +{tp:.0f}% / 총 {len(items)}건",
             ""]
    order = ALERT_KINDS + [k for k in by_kind if k not in ALERT_KINDS]
    for kind in order:
        s = settle_kind(by_kind.get(kind, []), sl, tp)
        label = KIND_LABEL.get(kind, kind)
        tag = "" if kind in ALERT_KINDS else " (알림 안 나감)"
        if s is None:
            lines.append(f"— <b>{label}</b>{tag}: 기록 없음")
            continue
        lines.append(
            f"— <b>{label}</b>{tag} n={s['n']} → {verdict(s)}\n"
            f"   규칙A(손절만) 기대값 <b>{s['a_ev']:+.1f}%</b>/건 · 손절률 {s['a_stop_pct']}%\n"
            f"   규칙B(±기준) {s['b_ev_pess']:+.1f}~{s['b_ev_opt']:+.1f}%/건"
            f" (모호 {s['b_ambiguous']}건) · 익절도달 {s['tp_pct']}% · 24h생존 {s['surv_pct']}%")
    lines.append("")
    lines.append("※ 규칙A는 기록만으로 확정 계산. 규칙B의 범위는 손절/익절 중")
    lines.append("   무엇이 먼저였는지 기록에 없어서 생기는 불확실성.")
    lines.append("※ perf:done은 최근 200건까지만 보관 — 정산은 그 범위 기준.")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--sl", type=float, default=20, help="손절 %% (기본 20)")
    ap.add_argument("--tp", type=float, default=20, help="익절 %% (기본 20)")
    ap.add_argument("--tg", action="store_true", help="텔레그램으로도 발송")
    args = ap.parse_args()

    report = build_report(args.sl, args.tp)
    # 콘솔용: 태그 제거
    print(report.replace("<b>", "").replace("</b>", ""))

    if args.tg:
        tok, chat = os.environ["TELEGRAM_BOT_TOKEN"], os.environ["TELEGRAM_CHAT_ID"]
        r = requests.post(f"https://api.telegram.org/bot{tok}/sendMessage",
                          json={"chat_id": chat, "text": report, "parse_mode": "HTML"},
                          timeout=10)
        print("[TG]", "발송 완료" if r.ok else f"실패 {r.status_code}: {r.text[:200]}")


if __name__ == "__main__":
    main()
