# 선케어 마켓 모니터링

**씨엠에스랩 셀퓨전씨** 브랜드의 네이버 쇼핑 시장을 자동으로 수집·분석하여 HTML 리포트를 생성하는 파이썬 스크립트입니다.

---

## 주요 기능

| 기능 | 설명 |
|------|------|
| **자사 SKU 자동 발견** | 네이버 쇼핑에서 브랜드×카테고리 검색 → 제목 정규화 → 동일 SKU 자동 병합 |
| **자사 노출 상태 확인** | own_query 기반 검색, 자사 상품 몇 개가 결과에 나오는지 추적 |
| **경쟁사 시장 분석** | 시드 키워드 + 브랜드별 검색 → 단품/묶음/비정상가 분류 |
| **가격 이상치 탐지** | 절대 최저가·ml당 P25·자사 공식가 3단 기준으로 비정상 상품 필터 |
| **네이버 DataLab 트렌드** | 선크림·선스틱 등 키워드 8주 검색 지수 수집·시각화 |
| **Claude AI 인사이트** | 수집 데이터를 Claude Sonnet에게 분석 요청 → 핵심 문장·액션 아이템 생성 |
| **HTML 리포트 자동 저장** | Jinja2 템플릿 기반, `reports/` 폴더에 타임스탬프 파일명으로 저장 |

---

## 데이터 파이프라인 (10 Layers)

```
Layer 0   load_config()                  config.json 로드
Layer 1   load_own_sku_master()          자사 SKU 마스터 로드 (JSON/CSV/config 순)
Layer 2   fetch_own_market_presence()    own_query 기반 자사 노출 확인
          collect_own_from_naver()       3단계 자동 발견 파이프라인 (API 사용 시)
Layer 3   collect_seed_market()          카테고리 일반 키워드 시장 풀 수집
Layer 4   collect_brand_market()         브랜드×카테고리 보강 수집
Layer 5   merge_market_candidates()      seed + brand 병합/중복 제거
Layer 6   classify_product()             점수 기반 분류 (single/promo/bundle/abnormal/unknown)
Layer 7   detect_price_outlier()         3단 이상치 탐지
Layer 8   build_category_price_reference()  ml당 참조가 계산 (avg/median/p25)
Layer 9   build_report_view_model()      템플릿용 뷰 모델 빌더
Layer 10  render_report()               Jinja2 렌더링 → HTML 출력
```

---

## 설치

### 1. Python 환경 준비

Python 3.10 이상을 권장합니다.

```bash
python -m venv .venv
.venv\Scripts\activate          # Windows
# source .venv/bin/activate     # macOS/Linux

pip install -r requirements.txt
```

### 2. 의존성 패키지

```
anthropic>=0.40.0       # Claude AI API
requests>=2.31.0        # 네이버 API 호출
beautifulsoup4>=4.12.0
jinja2>=3.1.0           # HTML 템플릿 렌더링
python-dotenv>=1.0.0    # .env 로드
```

### 3. API 키 설정

프로젝트 루트에 `.env` 파일을 생성합니다. (절대 Git에 커밋하지 마세요)

```env
NAVER_CLIENT_ID=네이버_API_클라이언트_ID
NAVER_CLIENT_SECRET=네이버_API_클라이언트_시크릿
ANTHROPIC_API_KEY=claude_api_키
```

#### 네이버 API 발급

1. [네이버 개발자센터](https://developers.naver.com) 로그인
2. Application 등록 → **검색** + **데이터랩 (검색어트렌드)** 권한 선택
3. 발급된 `Client ID`와 `Client Secret`을 `.env`에 입력

#### Claude API 발급

- [Anthropic Console](https://console.anthropic.com) 에서 API 키 발급

> **API 키 없이 실행할 경우** 자동으로 샘플 데이터를 사용해 리포트를 생성합니다.

---

## 실행

```bash
python monitor.py
```

실행 단계:
```
[1/4] 네이버 DataLab 트렌드 수집
[2/4] 자사 SKU 자동 발견 (Naver 검색)
[3/4] 경쟁사 시장 수집 (시드 + 브랜드 파이프라인)
[4/4] Claude AI 분석
[완료] 리포트 저장: reports/report_YYYYMMDD_HHMM.html
```

결과물은 `reports/` 폴더에 HTML 파일로 저장됩니다. 브라우저에서 바로 열 수 있습니다.

---

## 설정 파일 (config.json)

### 브랜드 / 기본 정보

```json
{
  "report_title": "선케어 마켓 모니터링",
  "company_name": "씨엠에스랩",
  "brand_name": "셀퓨전씨",
  "our_brand_keyword": "셀퓨전씨"
}
```

### 자사 SKU 정의

`products` 배열에 시리즈(series)별로 SKU를 정의합니다.

```json
{
  "products": [
    {
      "series": "레이저 UV",
      "skus": [
        {
          "name": "레이저 UV 썬스크린 50ml",
          "own_query": "셀퓨전씨 레이저 UV 썬스크린",
          "market_query": "선크림 SPF50 50ml",
          "official_price": 43000,
          "category": "선크림",
          "volume": "50ml",
          "status": "active"
        }
      ]
    }
  ]
}
```

| 필드 | 설명 |
|------|------|
| `own_query` | 자사 상품 검색에 사용하는 키워드 |
| `market_query` | 경쟁사 시장 검색에 사용하는 키워드 |
| `official_price` | 자사 공식 판매가 (이상치 탐지 기준) |
| `status` | `active` / `inactive` (inactive는 수집에서 제외) |

### 외부 SKU 마스터 파일 (권장)

`data/own_sku_master.json` 또는 `data/own_sku_master.csv`를 생성하면 config.json의 products 정의보다 우선 적용됩니다.

```json
[
  {
    "sku_id": "LUV-50",
    "name": "레이저 UV 썬스크린 50ml",
    "series": "레이저 UV",
    "category": "선크림",
    "volume_ml": 50,
    "official_price": 43000,
    "own_query": "셀퓨전씨 레이저 UV 썬스크린",
    "status": "active"
  }
]
```

### 경쟁사 설정

```json
{
  "competitor_brands": ["닥터지", "라로슈포제", "AHC", "달바", "이니스프리"],
  "exclude_keywords": ["다이소", "DAISO"],
  "exclude_brands": ["더마블록", "다이소"]
}
```

### 카테고리별 용량 범위 (이상치 탐지 기준)

```json
{
  "category_volume_ranges": {
    "선크림":    {"min_ml": 30,  "max_ml": 100},
    "선스틱":    {"min_ml": 10,  "max_ml": 30},
    "선세럼":    {"min_ml": 30,  "max_ml": 60},
    "선스프레이": {"min_ml": 80, "max_ml": 250}
  }
}
```

### 이상치 탐지 임계값

```json
{
  "category_price_rules": {
    "선크림": {"abs_min": 5000, "abs_max": 120000}
  },
  "abnormal_rules": {
    "own_ref_threshold": 0.4,
    "ml_p25_threshold": 0.5
  }
}
```

| 설정 | 설명 |
|------|------|
| `abs_min` | 이 금액 미만이면 Tier A 이상치로 판단 |
| `own_ref_threshold` | 자사 공식가의 이 비율 미만이면 Tier C 이상치 (기본 40%) |
| `ml_p25_threshold` | ml당 P25 가격의 이 비율 미만이면 Tier B 이상치 (기본 50%) |

2개 이상 Tier 충족 시 `confidence=high` → abnormal 분류
1개 Tier 충족 시 `confidence=low` → unknown 분류 (단품 강제 제외 없음)

---

## 상품 분류 기준 (Layer 6)

| 분류 | 조건 |
|------|------|
| `single` | 정상 용량 범위 + 정상 가격 + 프로모 키워드 없음 |
| `bundle` | 묶음 점수 ≥ 0.7 (N개입, xN, N+M 패턴 등) |
| `promo` | 묶음 점수 ≥ 0.4 또는 프로모 키워드 ≥ 0.5 |
| `abnormal` | 가격 점수 ≤ -0.7 (자사 공식가 대비 35% 미만 등) |
| `unknown` | 위 기준 미충족 |

---

## 리포트 구성

생성된 HTML 리포트는 다음 섹션으로 구성됩니다.

1. **시장 요약** — 핵심 지표 카드 (검색 지수 변화, 경쟁 상품 수, SKU 커버리지)
2. **Claude AI 인사이트** — 핵심 한 줄 요약, 시장 인사이트 3개, 액션 아이템, 리스크
3. **검색 트렌드 차트** — 키워드별 8주 검색 지수 추이 (Chart.js)
4. **자사 SKU 현황** — 시리즈별 카드, 시장 노출 상태, 경쟁가 대비 포지션
5. **카테고리별 경쟁사 분석** — 단품/행사/비정상 분포, ml당 가격 비교, 브랜드 포지셔닝
6. **데이터 수집 기준** — 수집 방법, 분류 기준, 데이터 범위 안내

---

## 프로젝트 구조

```
.
├── monitor.py              # 메인 스크립트 (전체 파이프라인)
├── config.json             # 브랜드·SKU·경쟁사 설정
├── requirements.txt        # Python 의존성
├── .env                    # API 키 (Git 제외)
├── data/
│   └── own_sku_master.json # 외부 SKU 마스터 (선택, config보다 우선)
├── templates/
│   └── report.html         # Jinja2 HTML 템플릿
└── reports/
    └── report_YYYYMMDD_HHMM.html  # 생성된 리포트
```

---

## 주의사항

- `.env` 파일에는 API 키가 포함되어 있으므로 **절대 Git에 커밋하지 마세요.** (`.gitignore`에 이미 포함)
- 네이버 검색 API는 하루 호출 한도가 있습니다. 과도한 반복 실행 시 한도 초과 가능.
- Claude API 사용 시 호출당 비용이 발생합니다. 시스템 프롬프트에 `cache_control: ephemeral`이 적용되어 캐시 히트 시 비용이 절감됩니다.
- 리포트의 모든 분석은 네이버 쇼핑 공개 가격 데이터 기반입니다. 재고·매출 데이터는 포함되지 않습니다.
