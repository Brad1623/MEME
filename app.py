# -*- coding: utf-8 -*-
"""
Render Web Service 래퍼 (무료 티어용)
- gunicorn app:app 으로 실행
- 스크리너 루프(main.main)는 백그라운드 스레드로 가동
- "/" 헬스체크 → UptimeRobot 5분 핑으로 슬립 방지
"""
import threading
import time
import json
import requests
from flask import Flask
import main as screener

app = Flask(__name__)

_lock = threading.Lock()
_worker = None          # 현재 스크리너 스레드 핸들
_watchdog_started = False


def r_status():
    """status 표시용 데이터를 파이프라인 1회로 모두 읽기.
    GET 6개 + 히스토리 LRANGE 1개. 타임아웃 짧게 → 워커 안 막음."""
    get_keys = [
        "status:cycle", "status:candidates", "status:last_scan",
        "status:alert_count", "status:last_alert", "status:restart_count",
    ]
    try:
        cmds = [["GET", k] for k in get_keys]
        cmds.append(["LRANGE", "status:history", 0, 19])
        cmds.append(["LRANGE", "archive:alerts", 0, 29])
        cmds.append(["GET", "pos:snapshot"])
        cmds.append(["GET", "perf:summary"])
        cmds.append(["GET", "perf:summary_shadow"])
        cmds.append(["GET", "perf:summary_doubledip"])
        cmds.append(["GET", "mkt:activity"])
        cmds.append(["GET", "trades:summary"])
        cmds.append(["LRANGE", "trades:done", 0, 4])
        resp = requests.post(
            screener.UPSTASH_URL + "/pipeline",
            headers={"Authorization": f"Bearer {screener.UPSTASH_TOKEN}"},
            json=cmds,
            timeout=3,
        )
        results = resp.json()
        vals = [r.get("result") if isinstance(r, dict) else None for r in results]
        d = dict(zip(get_keys, vals[:6]))
        d["history"] = vals[6] if len(vals) > 6 and vals[6] else []
        d["archive"] = vals[7] if len(vals) > 7 and vals[7] else []
        d["possnap"] = vals[8] if len(vals) > 8 else None
        d["perfsum"] = vals[9] if len(vals) > 9 else None
        d["shadowsum"] = vals[10] if len(vals) > 10 else None
        d["ddsum"] = vals[11] if len(vals) > 11 else None
        d["mktact"] = vals[12] if len(vals) > 12 else None
        d["tradesum"] = vals[13] if len(vals) > 13 else None
        d["trades"] = vals[14] if len(vals) > 14 and vals[14] else []
        return d
    except Exception:
        return {k: None for k in get_keys} | {"history": [], "archive": [], "possnap": None,
                                              "perfsum": None, "shadowsum": None, "ddsum": None,
                                              "mktact": None, "tradesum": None, "trades": []}


def _spawn_worker():
    """스크리너 스레드를 새로 띄운다."""
    global _worker
    _worker = threading.Thread(target=screener.main, daemon=True, name="screener")
    _worker.start()
    print("[app] screener thread spawned")


def _watchdog():
    """스크리너 스레드가 죽었는지 30초마다 감시. 죽었으면 되살린다."""
    global _worker
    while True:
        time.sleep(30)
        try:
            if _worker is None or not _worker.is_alive():
                print("[watchdog] screener thread dead → 재시작")
                _spawn_worker()
                # 재시작 횟수 기록 (status에서 확인 → 잦으면 문제 신호)
                try:
                    requests.post(
                        screener.UPSTASH_URL,
                        headers={"Authorization": f"Bearer {screener.UPSTASH_TOKEN}"},
                        json=["INCR", "status:restart_count"],
                        timeout=10,
                    )
                except Exception:
                    pass
        except Exception as e:
            print(f"[watchdog] error: {e}")


def start_once():
    """gunicorn import 시점에 워커 + 워치독 1회 기동."""
    global _watchdog_started
    with _lock:
        if _watchdog_started:
            return
        _spawn_worker()
        wd = threading.Thread(target=_watchdog, daemon=True, name="watchdog")
        wd.start()
        _watchdog_started = True
        print("[app] worker + watchdog started")


# gunicorn이 모듈을 import하는 시점에 기동
start_once()


@app.route("/")
def health():
    return "sol-meme-screener alive", 200


@app.route("/status")
def status():
    """봇이 실제로 스캔 중인지 + 후보수 흐름을 브라우저에서 확인."""
    d = r_status()
    cycle = d["status:cycle"] or "?"
    cand = d["status:candidates"] or "?"
    last_scan = d["status:last_scan"]
    alert_count = d["status:alert_count"] or "0"
    last_alert = d["status:last_alert"] or "아직 없음"
    restart_count = d["status:restart_count"] or "0"
    history = d.get("history") or []
    archive = d.get("archive") or []

    if last_scan:
        try:
            ago = int(time.time()) - int(float(last_scan))
        except (ValueError, TypeError):
            ago = 99999
        if ago < 180:
            health_txt = f"🟢 정상 (마지막 스캔 {ago}초 전)"
        elif ago < 600:
            health_txt = f"🟡 느림 (마지막 스캔 {ago}초 전 — 무료티어 슬립 가능)"
        else:
            health_txt = f"🔴 멈춤? (마지막 스캔 {ago//60}분 전)"
    else:
        health_txt = "⚪ 아직 첫 스캔 전"

    # 흐름 시각화: history 각 항목 "n:trend:new:boost" → 막대
    # history[0]이 최신이므로 뒤집어서 오래된→최신 순으로 그림
    bars = ""
    parsed = []
    for item in reversed(history):
        try:
            n, tr, nw, bo = (int(x) for x in item.split(":"))
            parsed.append((n, tr, nw, bo))
        except (ValueError, AttributeError):
            continue
    if parsed:
        mx = max((p[0] for p in parsed), default=1) or 1
        for n, tr, nw, bo in parsed:
            h = int(n / mx * 60) + 2  # 높이 2~62px
            color = "#e74c3c" if n == 0 else ("#f39c12" if n < 20 else "#2ecc71")
            bars += (f"<div title='후보 {n} (트렌드{tr}/신규{nw}/부스트{bo})' "
                     f"style='display:inline-block;width:13px;height:{h}px;"
                     f"background:{color};margin:0 1px;vertical-align:bottom;border-radius:2px 2px 0 0'></div>")
        # 소스별 최근 평균 (0이 아닌 것만)
        nonzero = [p for p in parsed if p[0] > 0]
        if nonzero:
            avg_tr = sum(p[1] for p in nonzero) / len(nonzero)
            avg_nw = sum(p[2] for p in nonzero) / len(nonzero)
            avg_bo = sum(p[3] for p in nonzero) / len(nonzero)
            zero_ct = sum(1 for p in parsed if p[0] == 0)
            src_summary = (f"최근 {len(parsed)}회 중 0개: <b>{zero_ct}회</b> · "
                           f"평균 트렌드 {avg_tr:.0f} / 신규 {avg_nw:.0f} / 부스트 {avg_bo:.0f}")
        else:
            src_summary = f"최근 {len(parsed)}회 전부 0개 — 소스 이상 의심 🔴"
    else:
        src_summary = "흐름 데이터 수집 중…"

    # 알림 아카이브 목록 HTML 생성
    arch_rows = ""
    pump_ct = dip_ct = 0
    for rec in archive:
        try:
            parts = rec.split("|")
            kind, sym, score, verdict, change, ts = parts[:6]
            icon = "🚨" if kind == "pump" else "🎯"
            if kind == "pump":
                pump_ct += 1
            else:
                dip_ct += 1
            score_txt = f" {score}점" if score not in ("-", "") else ""
            # 판정 색
            vcolor = "#2ecc71" if "양호" in verdict or "무난" in verdict else (
                "#e74c3c" if "주의" in verdict else "#f39c12")
            arch_rows += (
                f"<div style='padding:7px 0;border-bottom:1px solid #f0f0f0;font-size:0.9em'>"
                f"{icon} <b>{sym}</b>{score_txt} "
                f"<span style='color:{vcolor};font-size:0.85em'>{verdict}</span><br>"
                f"<span style='color:#999;font-size:0.8em'>{change} · {ts} UTC</span></div>"
            )
        except (ValueError, IndexError):
            continue
    if arch_rows:
        arch_block = (
            f"<div class='chart'>"
            f"<div class='k' style='font-size:0.9em;margin-bottom:10px'>"
            f"📜 최근 알림 기록 (🚨급등 {pump_ct} · 🎯눌림목 {dip_ct})</div>"
            f"{arch_rows}</div>"
        )
    else:
        arch_block = ""

    # 💼 보유 포지션 블록 (main이 매 사이클 기록한 스냅샷)
    pos_block = ""
    try:
        snap = json.loads(d.get("possnap") or "[]")
    except Exception:
        snap = []
    if snap:
        rows = ""
        for p in snap:
            risk = p.get("risk")
            rtxt = ""
            if risk is not None:
                rc = "#e74c3c" if risk >= 40 else ("#f39c12" if risk >= 20 else "#2ecc71")
                rtxt = f" <span style='color:{rc};font-size:0.85em'>위험도 {risk:.0f}</span>"
            if p.get("pnl") is not None:
                c = "#2ecc71" if p["pnl"] >= 0 else "#e74c3c"
                rows += (f"<div style='padding:6px 0;border-bottom:1px solid #f0f0f0'>"
                         f"<b>{p['sym']}</b>  ${p['entry']:g} → ${p['price']:g} "
                         f"<b style='color:{c}'>{p['pnl']:+.1f}%</b>{rtxt}</div>")
            else:
                rows += (f"<div style='padding:6px 0;border-bottom:1px solid #f0f0f0'>"
                         f"<b>{p.get('sym','?')}</b>  ${p.get('entry','?')} → 시세조회중</div>")
        pos_block = (f"<div class='chart'><div class='k' style='font-size:0.9em;margin-bottom:8px'>"
                     f"💼 보유 포지션 (매도감시 중 · 2분 주기)</div>{rows}"
                     f"<div class='k' style='font-size:0.75em;margin-top:6px'>"
                     f"텔레그램: /buy 민트 [가격] · /sell 민트 · /pos · /check 민트</div></div>")
    else:
        pos_block = (f"<div class='row'><span class='k'>💼 포지션:</span> 없음 "
                     f"<span style='color:#aaa;font-size:0.8em'>(텔레그램 /buy 민트주소 로 등록 → 매도시그널 감시)</span></div>")

    # 📈 알림 성과추적 블록 (+10% 가설 검증)
    perf_block = ""
    try:
        ps = json.loads(d.get("perfsum") or "{}")
    except Exception:
        ps = {}
    if ps.get("n"):
        hit_c = "#2ecc71" if ps.get("hit10_pct", 0) >= 50 else "#f39c12"
        def fmt(v):
            return f"{v:+.1f}%" if v is not None else "-"
        perf_block = (
            f"<div class='chart'><div class='k' style='font-size:0.9em;margin-bottom:8px'>"
            f"📈 알림 성과추적 (완료 {ps['n']}건 · 24h 관찰)</div>"
            f"<div style='font-size:1.05em;margin-bottom:6px'>+10% 도달률: "
            f"<b style='color:{hit_c}'>{ps.get('hit10_pct','?')}%</b></div>"
            f"<div style='font-size:0.88em;color:#555'>"
            f"중앙값 수익 — 1h {fmt(ps.get('med_r1'))} · 6h {fmt(ps.get('med_r6'))} · 24h {fmt(ps.get('med_r24'))}<br>"
            f"평균 최고점 {fmt(ps.get('avg_peak'))} · 최악 낙폭 {fmt(ps.get('worst'))}</div></div>"
        )
    else:
        perf_block = (f"<div class='row'><span class='k'>📈 성과추적:</span> 수집 중 "
                      f"<span style='color:#aaa;font-size:0.8em'>(알림 후 24h 지나면 집계 시작)</span></div>")

    # 🔬 20분 미만 섀도우 추적 (나이필터 20분 근거 검증용 — 알림 없이 조용히 비교)
    shadow_block = ""
    try:
        ss = json.loads(d.get("shadowsum") or "{}")
    except Exception:
        ss = {}
    if ss.get("n"):
        def fmt2(v):
            return f"{v:+.1f}%" if v is not None else "-"
        diff = None
        if ps.get("hit10_pct") is not None:
            diff = ss.get("hit10_pct", 0) - ps.get("hit10_pct", 0)
        diff_txt = ""
        if diff is not None:
            dc = "#2ecc71" if diff > 5 else ("#e74c3c" if diff < -5 else "#888")
            diff_txt = f" <span style='color:{dc}'>(정식알림 대비 {diff:+.0f}%p)</span>"
        shadow_block = (
            f"<div class='chart'><div class='k' style='font-size:0.9em;margin-bottom:8px'>"
            f"🔬 20분미만 섀도우추적 (완료 {ss['n']}건 · 알림無 · 나이필터 검증용)</div>"
            f"<div style='font-size:1.05em;margin-bottom:6px'>+10% 도달률: <b>{ss.get('hit10_pct','?')}%</b>{diff_txt}</div>"
            f"<div style='font-size:0.88em;color:#555'>"
            f"평균 최고점 {fmt2(ss.get('avg_peak'))} · 최악낙폭 {fmt2(ss.get('worst'))}</div>"
            f"<div class='k' style='font-size:0.75em;margin-top:6px'>표본 20건 이상부터 판단 권장 · MIN_AGE_MINUTES 조정 근거자료</div></div>"
        )
    else:
        shadow_block = (f"<div class='row'><span class='k'>🔬 섀도우추적(20분미만):</span> 수집 중 "
                        f"<span style='color:#aaa;font-size:0.8em'>(나이필터 근거 검증용, 알림 없음)</span></div>")

    # 🌀 2단 눌림(더블딥) 관찰 트래커 — 알림 없이 가설 검증 전용
    dd_block = ""
    try:
        dds = json.loads(d.get("ddsum") or "{}")
    except Exception:
        dds = {}
    if dds.get("n"):
        def fmt3(v):
            return f"{v:+.1f}%" if v is not None else "-"
        diff2 = None
        if ps.get("hit10_pct") is not None:
            diff2 = dds.get("hit10_pct", 0) - ps.get("hit10_pct", 0)
        diff2_txt = ""
        if diff2 is not None:
            dc2 = "#2ecc71" if diff2 > 5 else ("#e74c3c" if diff2 < -5 else "#888")
            diff2_txt = f" <span style='color:{dc2}'>(정식알림 대비 {diff2:+.0f}%p)</span>"
        dd_block = (
            f"<div class='chart'><div class='k' style='font-size:0.9em;margin-bottom:8px'>"
            f"🌀 2단눌림 관찰 (완료 {dds['n']}건 · 알림無 · 1차-10%→반등→2차-30% 가설검증)</div>"
            f"<div style='font-size:1.05em;margin-bottom:6px'>+10% 도달률: <b>{dds.get('hit10_pct','?')}%</b>{diff2_txt}</div>"
            f"<div style='font-size:0.88em;color:#555'>"
            f"평균 최고점 {fmt3(dds.get('avg_peak'))} · 최악낙폭 {fmt3(dds.get('worst'))}</div>"
            f"<div class='k' style='font-size:0.75em;margin-top:6px'>표본 20건+ 쌓이면 판단 · 검증되면 정식 알림 승격 검토</div></div>"
        )
    else:
        dd_block = (f"<div class='row'><span class='k'>🌀 2단눌림 관찰:</span> 수집 중 "
                    f"<span style='color:#aaa;font-size:0.8em'>(가설검증용, 알림 없음)</span></div>")

    # 🌐 시장 활성도 (거래량 신호 보정 지수)
    try:
        mact = float(d.get("mktact")) if d.get("mktact") else None
    except (ValueError, TypeError):
        mact = None
    if mact is not None:
        if mact < 0.8:
            mtxt, mc = f"한산 ({mact:.2f})", "#f39c12"
        elif mact < 1.3:
            mtxt, mc = f"평시 ({mact:.2f})", "#2ecc71"
        else:
            mtxt, mc = f"활발 ({mact:.2f})", "#3498db"
        mkt_block = (f"<div class='row'><span class='k'>🌐 시장 활성도:</span> "
                     f"<b style='color:{mc}'>{mtxt}</b> "
                     f"<span style='color:#aaa;font-size:0.8em'>(거래량 매도신호 자동보정)</span></div>")
    else:
        mkt_block = (f"<div class='row'><span class='k'>🌐 시장 활성도:</span> 측정 중 "
                     f"<span style='color:#aaa;font-size:0.8em'>(baseline 3시간 축적 필요)</span></div>")

    # 💰 실거래 기록 블록 (/sell 시 자동 저장 — 자기교정 데이터)
    trade_block = ""
    try:
        ts_sum = json.loads(d.get("tradesum") or "{}")
    except Exception:
        ts_sum = {}
    if ts_sum.get("n"):
        wc = "#2ecc71" if ts_sum.get("win_rate", 0) >= 50 else "#e74c3c"
        rows = ""
        for x in (d.get("trades") or []):
            try:
                t = json.loads(x)
                pc = "#2ecc71" if (t.get("pnl") or 0) >= 0 else "#e74c3c"
                pnl_s = f"{t['pnl']:+.1f}%" if t.get("pnl") is not None else "?"
                rows += (f"<div style='padding:5px 0;border-bottom:1px solid #f0f0f0;font-size:0.88em'>"
                         f"<b>{t.get('sym','?')}</b> <b style='color:{pc}'>{pnl_s}</b> "
                         f"<span style='color:#999;font-size:0.85em'>보유 {t.get('hold_min','?')}분 · {t.get('ts','')}</span></div>")
            except Exception:
                continue
        trade_block = (
            f"<div class='chart'><div class='k' style='font-size:0.9em;margin-bottom:8px'>"
            f"💰 실거래 기록 (누적 {ts_sum['n']}건)</div>"
            f"<div style='margin-bottom:6px'>승률 <b style='color:{wc}'>{ts_sum.get('win_rate','?')}%</b> · "
            f"평균익 +{ts_sum.get('avg_win','?')}% / 평균손 {ts_sum.get('avg_loss','?')}% · "
            f"합계 {ts_sum.get('total','?'):+g}%</div>{rows}</div>"
        )
    else:
        trade_block = (f"<div class='row'><span class='k'>💰 실거래:</span> 기록 없음 "
                       f"<span style='color:#aaa;font-size:0.8em'>(/sell 시 자동 저장)</span></div>")

    html = f"""<html><head><meta charset='utf-8'>
<meta http-equiv='refresh' content='30'>
<title>Meme Screener 상태</title>
<style>body{{font-family:-apple-system,sans-serif;max-width:500px;margin:40px auto;padding:0 20px;line-height:1.8}}
.big{{font-size:1.3em;font-weight:bold;margin:20px 0;padding:15px;background:#f4f4f4;border-radius:10px}}
.row{{padding:8px 0;border-bottom:1px solid #eee}}
.k{{color:#888}}
.chart{{margin:20px 0;padding:15px;background:#fafafa;border-radius:10px}}
.barbox{{height:64px;border-bottom:1px solid #ddd;padding-bottom:0}}</style></head><body>
<h2>🤖 Solana Meme Screener</h2>
<div class='big'>{health_txt}</div>
<div class='row'><span class='k'>스캔 사이클:</span> <b>{cycle}</b> 회</div>
<div class='row'><span class='k'>마지막 후보 수:</span> <b>{cand}</b> 개</div>
<div class='chart'>
  <div class='k' style='font-size:0.9em;margin-bottom:8px'>📊 최근 후보수 흐름 (오래된 → 최신)</div>
  <div class='barbox'>{bars}</div>
  <div class='k' style='font-size:0.82em;margin-top:8px'>{src_summary}</div>
  <div class='k' style='font-size:0.75em;margin-top:4px'>🟢 20+ · 🟡 1~19 · 🔴 0 (막대에 마우스대면 상세)</div>
</div>
<div class='row'><span class='k'>누적 알림:</span> <b>{alert_count}</b> 회</div>
<div class='row'><span class='k'>마지막 알림:</span> {last_alert}</div>
<div class='row'><span class='k'>스레드 재시작:</span> <b>{restart_count}</b> 회 <span style='color:#aaa;font-size:0.8em'>(잦으면 문제)</span></div>
{mkt_block}
{pos_block}
{perf_block}
{shadow_block}
{dd_block}
{trade_block}
{arch_block}
<p style='color:#aaa;font-size:0.85em;margin-top:30px'>30초마다 자동 새로고침 · 사이클 숫자가 계속 올라가면 정상</p>

<details style='margin-top:20px;padding:15px;background:#fafafa;border-radius:10px'>
<summary style='cursor:pointer;font-weight:bold;font-size:0.95em'>📖 기준표 (점수 배점 · 태그 기준)</summary>
<div style='margin-top:15px;font-size:0.85em;line-height:1.7'>

<div style='font-weight:bold;margin-bottom:6px'>📢 발송 근거 = 점수 (65점 이상만 알림)</div>
<table style='width:100%;border-collapse:collapse'>
<tr style='color:#888;font-size:0.9em'><td>항목</td><td style='text-align:right'>최대</td><td style='text-align:right'>만점조건</td></tr>
<tr><td>거래량 급증</td><td style='text-align:right'><b>30</b></td><td style='text-align:right;color:#888'>시간평균 5배</td></tr>
<tr><td>1h 가격모멘텀</td><td style='text-align:right'><b>20</b></td><td style='text-align:right;color:#888'>1h +100%</td></tr>
<tr><td>매수 압력</td><td style='text-align:right'><b>15</b></td><td style='text-align:right;color:#888'>매수비율 100%</td></tr>
<tr><td>5분 모멘텀</td><td style='text-align:right'><b>10</b></td><td style='text-align:right;color:#888'>5m +10%</td></tr>
<tr><td>EMA 골든크로스</td><td style='text-align:right'><b>10</b></td><td style='text-align:right;color:#888'>방금 교차(정배열 5)</td></tr>
<tr><td>레딧 언급증가</td><td style='text-align:right'><b>10</b></td><td style='text-align:right;color:#888'>시간당 +3건</td></tr>
<tr><td>부스트</td><td style='text-align:right'><b>5</b></td><td style='text-align:right;color:#888'>유료홍보중</td></tr>
<tr><td>신규상장</td><td style='text-align:right'><b>5</b></td><td style='text-align:right;color:#888'>48h 이내</td></tr>
</table>
<div style='color:#888;font-size:0.85em;margin-top:4px'>* 눌림목은 점수 아닌 조건통과제 (추세+얕은눌림+매수우세+거래량유지)</div>

<div style='font-weight:bold;margin:14px 0 6px'>📊 참고 정보 = 전문가 태그 (발송에 영향 없음)</div>
<div><b>📋 종합판정</b> — 👍양호(긍정2+·주의0) / 🙂무난 / 😐혼조 / 👎주의(주의2+ 또는 봇펌핑·사기홀더)</div>
<table style='width:100%;border-collapse:collapse;margin-top:6px'>
<tr style='color:#888;font-size:0.9em'><td>태그</td><td style='text-align:right'>기준</td></tr>
<tr><td>⭐ 스위트스팟</td><td style='text-align:right;color:#888'>시총 1M~5M</td></tr>
<tr><td>✨ 진입가능</td><td style='text-align:right;color:#888'>시총 0.5M~10M</td></tr>
<tr><td>🔬 초저시총 ⚠️</td><td style='text-align:right;color:#888'>시총 &lt;0.5M</td></tr>
<tr><td>🐋 고시총 ⚠️</td><td style='text-align:right;color:#888'>시총 &gt;10M</td></tr>
<tr><td>🔁 거래활발</td><td style='text-align:right;color:#888'>1h 250건+</td></tr>
<tr><td>👥 홀더분산</td><td style='text-align:right;color:#888'>상위홀더 ≤20%</td></tr>
<tr><td>🔓 다소집중 ⚠️</td><td style='text-align:right;color:#888'>20~50%</td></tr>
<tr><td>🚫 초집중 ⚠️</td><td style='text-align:right;color:#888'>≥50% 사기수준</td></tr>
<tr><td>⚠️ 봇펌핑의심</td><td style='text-align:right;color:#888'>20분내 +100%</td></tr>
<tr><td>🏆 검증된코인</td><td style='text-align:right;color:#888'>7일+ 생존(눌림목)</td></tr>
<tr><td>💬 커뮤니티활성</td><td style='text-align:right;color:#888'>레딧 언급(눌림목)</td></tr>
</table>

<div style='font-weight:bold;margin:14px 0 6px'>💡 인사이트</div>
<div style='color:#555'>과열도 = 1h거래량÷유동성 (5배+ 급락위험) · 위험도판정 = 종합 한줄</div>

<div style='font-weight:bold;margin:14px 0 6px'>🎖 진입등급 (급등 알림 첫줄 · 판단 요약)</div>
<div style='color:#555'>
<b>S</b> 점수75+ & 👍양호 & 과열아님 → 적극 검토<br>
<b>A</b> 65+ & 양호/무난 → 표준 진입<br>
<b>B</b> 😐혼조 또는 🔥과열 → 소액만<br>
<b>C</b> 👎주의 → 관망 권장</div>

<div style='font-weight:bold;margin:14px 0 6px'>🛑/🎯 손절·익절 트랙 (위험도와 별개)</div>
<div style='color:#555'>손절: 진입 -20% 기본 (/buy 3번째 인자로 변경) · 익절: +20% 도달 시 검토 알림 · 둘 다 15~30분 쿨다운</div>

<div style='font-weight:bold;margin:14px 0 6px'>⏱ 나이 필터 (20분 미만 제외 — 검증 진행중)</div>
<div style='color:#555'>정식 알림은 상장 20분+ 코인만. 20분 미만은 <b>알림 없이 섀도우 추적</b>만 하여 이 기준이 실제로 유효한지 데이터로 검증 중(위 🔬 섀도우추적 항목).</div>

<div style='font-weight:bold;margin:14px 0 6px'>🌀 2단눌림 가설 (2026-07-21 관찰 시작)</div>
<div style='color:#555'>
대상: 시총 $500K+ (전문가 스위트스팟 하한)<br>
① 고점 대비 -10% 이내 1차 눌림 → ② 반등 → ③ 반등고점 대비 -30% 2차 눌림 (①→③ 1시간 이내)<br>
가설: 이 2차 눌림 시점이 좋은 진입점 — <b>아직 미검증, 알림 없이 forward-test 중</b>(위 🌀 항목)</div>

<div style='font-weight:bold;margin:14px 0 6px'>🔻 매도 위험도 (포지션 감시 · 100점 만점)</div>
<table style='width:100%;border-collapse:collapse'>
<tr style='color:#888;font-size:0.9em'><td>신호</td><td style='text-align:right'>최대</td><td style='text-align:right'>발동 조건</td></tr>
<tr><td>🚨 급락</td><td style='text-align:right'><b>50</b></td><td style='text-align:right;color:#888'>5m -15%↓ (깊을수록↑)</td></tr>
<tr><td>📉 매수세 붕괴</td><td style='text-align:right'><b>25</b></td><td style='text-align:right;color:#888'>매수비율 45%↓</td></tr>
<tr><td>💤 거래량 소멸</td><td style='text-align:right'><b>20</b></td><td style='text-align:right;color:#888'>기준 0.5x↓ <b>※시간보정</b></td></tr>
<tr><td>↩️ 모멘텀 반전</td><td style='text-align:right'><b>15</b></td><td style='text-align:right;color:#888'>5m·1h 동시 마이너스</td></tr>
</table>
<div style='color:#555;margin-top:5px'><b>40점 이상</b>이면 매도 알림 · 위험도가 <b>+20점 급등</b>하면 쿨다운 무시하고 재알림</div>
<div style='color:#555;margin-top:3px'>🎯 익절 알림은 별도 트랙 (진입 대비 +20% 도달 시)</div>

<div style='font-weight:bold;margin:14px 0 6px'>🌐 시장 활성도 보정 (거래량 신호 전용)</div>
<div style='color:#555'>
미국 새벽 등 <b>모든 코인</b>이 한산한 시간엔 거래량 감소가 정상이라, 이걸 "관심 이탈"로 오판하지 않도록 자동 보정.<br>
· 활성도 = <b>지금 시장 중앙 거래량비 ÷ 최근 24h 시간대 baseline</b><br>
· 1.0 = 평시 · 0.5 = 한산 · 1.3+ = 활발<br>
· 한산하면 <b>기준도 낮추고 배점도 축소</b> (0.5면 기준 0.25x, 배점 절반)<br>
· 단, 시장은 보통인데 <b>그 코인만</b> 말랐으면 여전히 감지<br>
<span style='color:#888;font-size:0.9em'>* baseline 3시간 미만이면 보정 없이 원래 기준 사용</span>
</div>

</div>
</details>
</body></html>"""
    return html, 200


@app.route("/export")
def export():
    """perf/archive/trades 원본 덤프 — 배점 자기교정 분석용 (읽기전용)."""
    try:
        cmds = [
            ["LRANGE", "perf:done", 0, 199],
            ["LRANGE", "archive:alerts", 0, 49],
            ["LRANGE", "trades:done", 0, 99],
            ["GET", "perf:summary"],
            ["GET", "perf:summary_shadow"],
            ["GET", "perf:summary_doubledip"],
        ]
        resp = requests.post(
            screener.UPSTASH_URL + "/pipeline",
            headers={"Authorization": f"Bearer {screener.UPSTASH_TOKEN}"},
            json=cmds, timeout=5,
        )
        vals = [r.get("result") if isinstance(r, dict) else None for r in resp.json()]
        return {
            "perf_done": vals[0] or [],
            "archive": vals[1] or [],
            "trades": vals[2] or [],
            "perf_summary": vals[3],
            "perf_summary_shadow": vals[4],
            "perf_summary_doubledip": vals[5],
        }, 200
    except Exception as e:
        return {"error": str(e)}, 500


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=10000)
