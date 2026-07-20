# Solana Meme Screener v1.0

24시간 솔라나 밈코인 급등 후보 스크리닝 봇. DexScreener + GeckoTerminal + RugCheck + Reddit 데이터를 결합해 점수화하고, 임계값을 넘으면 텔레그램으로 알림을 보낸다. 팬텀 지갑 스왑용 Jupiter 링크 포함.

## 스코어링 구조 (0~100)

| 시그널 | 최대 배점 | 소스 |
|---|---|---|
| 거래량 급증 (1h vs 24h 시간평균, 5배=만점) | 30 | DexScreener |
| 1h 가격 모멘텀 (+100%=만점) | 20 | DexScreener |
| 매수 압력 (1h 매수비율) | 15 | DexScreener |
| 5분 모멘텀 | 10 | DexScreener |
| 5분봉 EMA9/21 골든크로스 (정배열 5점) | 10 | GeckoTerminal |
| 레딧 언급 시간당 증가 | 10 | Reddit |
| DexScreener 부스트 활성 | 5 | DexScreener |
| 신규 상장 (48h 이내) | 5 | DexScreener |

**하드 필터 (점수와 무관하게 제외):**
- 유동성 < $30k / 24h 거래량 < $100k
- 상장 30분 미만 (스나이핑봇 구간) 또는 30일 초과
- RugCheck `danger` 플래그 존재 (mint authority 미소각, freeze authority, LP 미소각 등)

## Render 배포 (기존 패턴 그대로)

1. GitHub 웹 UI로 새 repo 생성 → `main.py`, `requirements.txt` 업로드
2. Render → New → **Background Worker** (웹서비스 아님, 포트 불필요)
   - Build: `pip install -r requirements.txt`
   - Start: `python main.py`
3. 환경변수 등록:

| 변수 | 필수 | 기본값 |
|---|---|---|
| `TELEGRAM_BOT_TOKEN` | ✅ | - |
| `TELEGRAM_CHAT_ID` | ✅ | - |
| `UPSTASH_REDIS_REST_URL` | ✅ | - |
| `UPSTASH_REDIS_REST_TOKEN` | ✅ | - |
| `ALERT_SCORE` | | 65 |
| `MIN_LIQUIDITY_USD` | | 30000 |
| `MIN_VOL24_USD` | | 100000 |
| `MIN_AGE_MINUTES` | | 30 |
| `MAX_AGE_DAYS` | | 30 |
| `ALERT_COOLDOWN_SEC` | | 21600 (6h) |
| `LOOP_SECONDS` | | 120 |

주의: Render Background Worker는 무료 플랜이 없다 (Starter $7/mo). 무료로 돌리려면 기존처럼 Flask 웹서비스로 감싸고 UptimeRobot 핑을 쓰는 방식으로 개조 가능.

## 튜닝 가이드

- **알림 너무 많으면**: `ALERT_SCORE` 70~75로 상향, `MIN_LIQUIDITY_USD` 50000
- **알림 너무 적으면**: `ALERT_SCORE` 55~60, `MIN_VOL24_USD` 50000
- 처음 2~3일은 알림만 받으면서 실제 이후 흐름을 기록해서 임계값을 캘리브레이션할 것 (지표 검증하던 방식 그대로)

## v1 한계 및 v2 후보

- **X(트위터) 언급량**: API 유료화($200/mo)로 제외. LunarCrush 유료 플랜이 대안
- **텔레그램 방 인원 추적**: Telethon 유저봇으로 방을 직접 조인해야 가능. 추적 대상 방 리스트 관리 필요 → v2
- **레딧 매칭**: `$SYMBOL` 정확 매칭만 사용 (짧은 심볼 오탐 방지). 대형 코인은 잡히지만 극초기 코인은 언급 자체가 없어서 이 신호는 후행성이 강함
- **Pump.fun 신규 발행 감지**: 공식 API 없음. bitquery/헬리우스 웹훅으로 가능하나 노이즈가 극심해 v1 제외

## 리스크 고지

이 스크리너는 **이미 움직이기 시작한 코인**을 잡는 후행 도구다. 알림 시점은 보통 펌핑 초~중반이며, 도달 시점에 이미 고점인 경우도 많다. RugCheck danger 필터를 통과해도 소프트 러그(개발자 덤핑)는 막지 못한다. 밈코인은 전액 손실 가능 자산으로 취급하고 포지션 사이징을 우선할 것.
