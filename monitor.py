"""
monitor.py — 선케어 마켓 모니터링
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
데이터 파이프라인 (10개 계층):

  Layer 1  SKU Master          load_own_sku_master()
  Layer 2  Own Presence        fetch_own_market_presence()
  Layer 3  Seed Market         collect_seed_market()
  Layer 4  Brand Market        collect_brand_market()
  Layer 5  Merge + Dedup       merge_market_candidates()
  Layer 6  Classification      classify_product()          (점수 기반)
  Layer 7  Outlier Detection   detect_price_outlier()      (3단 기준)
  Layer 8  Reference Prices    build_category_price_reference()
  Layer 9  View Model          build_report_view_model()
  Layer 10 Render              render_report()
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""

import os
import re
import csv
import json
import html
import requests
from datetime import datetime, timedelta
from pathlib import Path
from jinja2 import Environment, FileSystemLoader
import anthropic
from dotenv import load_dotenv, find_dotenv

load_dotenv(find_dotenv(), override=True)

ANTHROPIC_API_KEY   = os.getenv("ANTHROPIC_API_KEY")
NAVER_CLIENT_ID     = os.getenv("NAVER_CLIENT_ID")
NAVER_CLIENT_SECRET = os.getenv("NAVER_CLIENT_SECRET")

NAVER_HEADERS = lambda: {
    "X-Naver-Client-Id":     NAVER_CLIENT_ID,
    "X-Naver-Client-Secret": NAVER_CLIENT_SECRET,
}

# ── 전역 정규식 ──────────────────────────────────────────────────────────────
_ML_RE           = re.compile(r"(\d+(?:\.\d+)?)\s*(?:ml|ML|mL|mL|㎖)\b", re.IGNORECASE)
_G_RE            = re.compile(r"(\d+(?:\.\d+)?)\s*(?:g|G|그램)\b")
_PACK_PLUS_RE    = re.compile(r"(\d+)\s*\+\s*(\d+)")
_PACK_ML_PLUS_RE = re.compile(r"\d+\s*(?:ml|g|㎖)\s*\+", re.IGNORECASE)  # "50ml+" 번들
_PACK_COUNT_RE   = re.compile(r"(?<!\d)([2-9])\s*(?:개|입|팩|병|통)")  # \b 제거: 한글은 \w
_PACK_X_RE       = re.compile(r"[xX×*]\s*([2-9])")                     # \b 제거: 한글은 \w
_PACK_KW_RE      = re.compile(r"(세트|기획|묶음|패키지|pack)", re.IGNORECASE)
_NOISE_PROMO_RE  = re.compile(r"(증정|사은품|이벤트|특가|체험|샘플)")
_NOISE_MISC_RE   = re.compile(r"(리뉴얼|대용량|리필)")
_BRACKET_RE      = re.compile(r"[\[\(][^\]\)]*[\]\)]")  # 제목 앞 [쿠팡], (이벤트) 등

# ── 자사 SKU 발견: 제목 정규화 전용 ─────────────────────────────────────────
_BRACKET_DECO_RE = re.compile(r"[\[\(][^\]\)]{0,50}[\]\)]")    # [NEW/시원촉촉], (신형)
_SPF_PA_RE       = re.compile(r"SPF\s*\d+\+*|PA\+{1,4}", re.IGNORECASE)
_UNIT_SINGLE_RE  = re.compile(r"(?<!\d)1\s*개(?!\d)")           # "1개" 단품 표기 제거
_COMMA_TRAIL_RE  = re.compile(r",\s*.{1,80}$")                  # ", SPF50+, 1개" 트레일링
_NON_WORD_RE     = re.compile(r"[^가-힣a-zA-Z0-9\s]")

# 카테고리 추론 테이블 (구체적인 것 먼저 — 선스틱 > 선세럼 > 선스프레이 > 선크림)
_CAT_KW_TABLE: list[tuple[str, list[str]]] = [
    ("선스틱",    ["썬스틱", "선스틱", "썬 스틱", "선 스틱"]),
    ("선세럼",    ["썬세럼", "선세럼", "썬 세럼", "선 세럼", "세럼"]),
    ("선스프레이", ["썬스프레이", "선스프레이", "썬 스프레이", "선 스프레이", "스프레이"]),
    ("선크림",    ["썬스크린", "선스크린", "선크림", "썬크림", "선 크림", "썬 크림"]),
]

# 시리즈 추출 시 제거할 제품 유형 용어 (긴 것 먼저 처리해야 부분 치환 방지)
_PROD_TYPE_TERMS: list[str] = sorted([
    "썬스크린", "선스크린", "선크림", "썬크림", "선 크림", "썬 크림",
    "썬세럼", "선세럼", "선 세럼", "썬 세럼", "세럼",
    "썬스틱", "선스틱", "선 스틱", "썬 스틱", "스틱",
    "썬스프레이", "선스프레이", "선 스프레이", "썬 스프레이", "스프레이",
    "선케어", "썬케어",
], key=len, reverse=True)

# ══════════════════════════════════════════════════════════════════════════════
# LAYER 0 — 설정 로드
# ══════════════════════════════════════════════════════════════════════════════

def load_config() -> dict:
    with open("config.json", encoding="utf-8") as f:
        return json.load(f)


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 1 — SKU MASTER
# ══════════════════════════════════════════════════════════════════════════════
#
# 우선순위:
#   1. data/own_sku_master.json   (external JSON 마스터)
#   2. data/own_sku_master.csv    (external CSV 마스터)
#   3. config.json products 폴백  (하위 호환)
#
# load_own_sku_master() → (list[dict], meta_dict) 반환
#   meta_dict: {source, file_path, total_defined, active}
# ══════════════════════════════════════════════════════════════════════════════

_SKU_MASTER_CANDIDATES = [
    Path("data/own_sku_master.json"),
    Path("data/own_sku_master.csv"),
]


def _build_default_market_query(sku: dict, mq_templates: dict) -> str:
    """market_query 미정의 시 카테고리+용량 기반 기본 쿼리."""
    cat = sku.get("category", "")
    templates = mq_templates.get(cat, [])
    if templates:
        return templates[0]
    vol_str = str(sku.get("volume") or "")
    vm = re.search(r"(\d+)", vol_str)
    vol_part = f" {vm.group(1)}ml" if vm else ""
    return f"{cat}{vol_part}" if cat else sku.get("name", "")


def _normalize_sku_row(row: dict, brand: str, mq_templates: dict) -> dict:
    """
    원시 SKU 행(config / JSON / CSV 공통)을 표준 형식으로 정규화.
    모든 SKU 정규화의 단일 진실 원천(single source of truth).

    필수 입력 필드: name, category
    선택 입력 필드: sku_id, series, volume_ml, volume, official_price/our_price,
                   status, own_query, market_query, search_query, subcategory, tags
    """
    # 용량 파싱: volume_ml 우선, 없으면 volume 문자열에서 추출
    raw_ml = row.get("volume_ml")
    if raw_ml not in (None, "", "0", 0):
        try:
            volume_ml = float(str(raw_ml).replace(",", ""))
        except (ValueError, TypeError):
            volume_ml = 0.0
    else:
        vol_str_src = str(row.get("volume") or "")
        vm = re.search(r"(\d+(?:\.\d+)?)", vol_str_src)
        volume_ml = float(vm.group(1)) if vm else 0.0

    vol_str = str(row.get("volume") or "") or (f"{volume_ml:.0f}ml" if volume_ml else "")

    # 가격 정규화 (문자열 "43,000" 등 CSV 값도 처리)
    raw_price = row.get("official_price") or row.get("our_price") or 0
    try:
        official_price = int(float(str(raw_price).replace(",", ""))) if raw_price else 0
    except (ValueError, TypeError):
        official_price = 0

    # own_query 폴백 체인
    own_query = (
        row.get("own_query") or
        row.get("search_query") or
        f"{brand} {row.get('name', '')}"
    ).strip()

    # market_query 폴백 체인
    market_query = (
        row.get("market_query") or
        _build_default_market_query(row, mq_templates)
    ).strip()

    series = str(row.get("series") or "기타").strip()
    name   = str(row.get("name")   or "").strip()

    # tags: JSON 배열 또는 쉼표 구분 문자열 지원
    raw_tags = row.get("tags")
    if isinstance(raw_tags, list):
        tags = raw_tags
    elif isinstance(raw_tags, str) and raw_tags.strip():
        tags = [t.strip() for t in raw_tags.split(",") if t.strip()]
    else:
        tags = []

    return {
        # 표준 필드
        "sku_id":         str(row.get("sku_id") or f"{series}_{name}"),
        "name":           name,
        "series":         series,
        "category":       str(row.get("category") or "").strip(),
        "subcategory":    str(row.get("subcategory") or "").strip(),
        "official_price": official_price,
        "volume_ml":      volume_ml,
        "status":         str(row.get("status") or "active").strip(),
        "own_query":      own_query,
        "market_query":   market_query,
        "tags":           tags,
        # 하위 호환 필드
        "our_price":      official_price,
        "volume":         vol_str,
        "search_query":   row.get("search_query") or own_query,
    }


def _load_skus_from_config(config: dict) -> tuple[list[dict], int]:
    """
    config.json products 섹션 → 표준 SKU 리스트.
    반환: (active_skus, total_defined_count)
    """
    brand        = config.get("our_brand_keyword", "")
    mq_templates = config.get("market_query_templates", {})
    flat: list[dict] = []
    total = 0

    for product in config.get("products", []):
        series = product.get("series", "기타")
        for raw_sku in product.get("skus", []):
            total += 1
            if raw_sku.get("status", "active") == "inactive":
                continue
            row = dict(raw_sku)
            row.setdefault("series", series)
            flat.append(_normalize_sku_row(row, brand, mq_templates))

    return flat, total


def _load_external_sku_file(config: dict) -> "tuple[list[dict], dict] | None":
    """
    data/ 폴더 외부 SKU 파일 로드 시도 (JSON → CSV 우선순위).
    파일 없거나 로드 실패 시 None 반환.
    """
    brand        = config.get("our_brand_keyword", "")
    mq_templates = config.get("market_query_templates", {})

    for path in _SKU_MASTER_CANDIDATES:
        if not path.exists():
            continue

        try:
            if path.suffix == ".json":
                with open(path, encoding="utf-8") as f:
                    raw = json.load(f)
                # 최상위 배열 또는 {"skus": [...]} 형식 모두 지원
                rows   = raw if isinstance(raw, list) else raw.get("skus", [])
                source = "external_json"
            elif path.suffix == ".csv":
                # utf-8-sig: BOM 있는 Excel 저장 CSV도 처리
                with open(path, encoding="utf-8-sig") as f:
                    reader = csv.DictReader(f)
                    rows   = list(reader)
                source = "external_csv"
            else:
                continue
        except Exception as e:
            print(f"  [SKU 마스터 경고] {path} 로드 실패: {e}")
            continue

        total_defined = len(rows)
        skus: list[dict] = []
        for row in rows:
            if str(row.get("status") or "active").strip() == "inactive":
                continue
            skus.append(_normalize_sku_row(row, brand, mq_templates))

        meta = {
            "source":        source,
            "file_path":     str(path),
            "total_defined": total_defined,
            "active":        len(skus),
        }
        print(f"  [SKU 마스터] {source} | {path} | {total_defined}개 정의 / {len(skus)}개 active")
        return skus, meta

    return None


def load_own_sku_master(config: dict) -> "tuple[list[dict], dict]":
    """
    SKU 마스터 로드. source of truth 우선순위:
      1. data/own_sku_master.json  (외부 JSON)
      2. data/own_sku_master.csv   (외부 CSV)
      3. config.json products      (폴백 — config에 정의된 SKU만 반영)

    반환: (active_skus: list[dict], meta: dict)
      meta 키: source, file_path, total_defined, active

    ※ config 폴백일 때 total_defined == len(active_skus)이면
       "config에 추가해야 반영됨"을 의미. 외부 파일로 교체해야 전체 SKU 반영 가능.
    """
    # 1. 외부 파일 시도
    result = _load_external_sku_file(config)
    if result is not None:
        return result

    # 2. config.json 폴백
    skus, total_defined = _load_skus_from_config(config)
    meta = {
        "source":        "config_fallback",
        "file_path":     "config.json",
        "total_defined": total_defined,
        "active":        len(skus),
    }
    return skus, meta


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 2 — OWN MARKET PRESENCE
# ══════════════════════════════════════════════════════════════════════════════

def _classify_comp(d: dict) -> dict:
    """자사 SKU 시장 노출 상태 분류."""
    if not d.get("has_data"):
        return {
            "code": "no_match",
            "label": "검색 결과 없음",
            "desc": "own_query 검색 결과 없음 - 키워드 점검 필요",
        }
    # 신규 방식: own_query_hits 기반
    own_hits = d.get("own_query_hits")
    if own_hits is not None:
        if own_hits == 0:
            return {
                "code": "market_only",
                "label": "자사 미노출",
                "desc": "검색 결과에 자사 상품 미포함 - 노출 개선 필요",
            }
        return {
            "code": "ok",
            "label": "노출 정상",
            "desc": f"자사 상품 {own_hits}개 검색 노출",
        }
    # 레거시 방식: comp_items 기반 (샘플 데이터 호환)
    comp = d.get("comp_items") or []
    if not comp:
        return {
            "code": "market_only",
            "label": "데이터 부족",
            "desc": "직접 검색 매칭 없음 · 카테고리 평균가 사용",
        }
    return {"code": "ok", "label": "정상", "desc": "데이터 수집 완료"}


def _empty_sku_result(sku: dict) -> dict:
    r = {
        "sku_id":         sku.get("sku_id", ""),
        "name":           sku.get("name", ""),
        "series":         sku.get("series", ""),
        "category":       sku.get("category", ""),
        "volume":         sku.get("volume", ""),
        "volume_ml":      sku.get("volume_ml", 0),
        "official_price": sku.get("official_price") or sku.get("our_price"),
        "our_price":      sku.get("official_price") or sku.get("our_price"),
        "own_query":      sku.get("own_query", ""),
        "market_query":   sku.get("market_query", ""),
        "market_items":   [], "our_items": [],
        "own_query_hits": 0, "total_hits": 0,
        "min_price": 0, "max_price": 0, "our_min_price": 0,
        "has_data": False,
    }
    r["comp_status"] = _classify_comp(r)
    return r


def fetch_own_market_presence(
    sku: dict,
    own_brand: str,
    exclude: list[str],
    count: int = 6,
) -> dict:
    """
    Layer 2: own_query 기반 자사 상품 노출 확인.
    경쟁사 데이터와 혼재하지 않음 — 자사 SKU 전용.
    """
    url = "https://openapi.naver.com/v1/search/shop.json"
    own_query = sku.get("own_query") or sku.get("search_query") or sku.get("name", "")
    params = {"query": own_query, "display": count, "sort": "asc"}

    try:
        resp = requests.get(url, headers=NAVER_HEADERS(), params=params, timeout=10)
        if resp.status_code != 200:
            print(f"  [경고] 쇼핑 API {resp.status_code}")
            return _empty_sku_result(sku)
        items_raw = resp.json().get("items", [])
    except Exception as e:
        print(f"  [경고] 쇼핑 API 오류: {e}")
        return _empty_sku_result(sku)

    items = []
    for item in items_raw:
        title = _clean_title(item.get("title", ""))
        mall  = item.get("mallName", "")
        price = int(item.get("lprice") or 0)
        if _is_excluded(title, mall, exclude) or price == 0:
            continue
        is_ours = bool(own_brand and (own_brand in title or own_brand in mall))
        items.append({
            "title": title, "mall": mall, "price": price,
            "link":  item.get("link", ""), "is_ours": is_ours,
        })

    our_items  = [i for i in items if i["is_ours"]]
    prices     = [i["price"] for i in items]

    result = {
        "sku_id":         sku.get("sku_id", ""),
        "name":           sku.get("name", ""),
        "series":         sku.get("series", ""),
        "category":       sku.get("category", ""),
        "volume":         sku.get("volume", ""),
        "volume_ml":      sku.get("volume_ml", 0),
        "official_price": sku.get("official_price") or sku.get("our_price"),
        "our_price":      sku.get("official_price") or sku.get("our_price"),
        "own_query":      own_query,
        "market_query":   sku.get("market_query", ""),
        "market_items":   items,
        "our_items":      our_items,
        "own_query_hits": len(our_items),
        "total_hits":     len(items),
        "min_price":      min(prices, default=0),
        "max_price":      max(prices, default=0),
        "our_min_price":  min((i["price"] for i in our_items), default=0),
        "has_data":       len(items) > 0,
    }
    result["comp_status"] = _classify_comp(result)
    return result


def normalize_product_title(title: str, brand: str) -> dict:
    """
    제품 제목 정규화.
    브랜드명·장식어(NEW, 시즌 태그 등)·SPF/PA·용량 단위를 분리해 canonical_name 추출.

    Returns: {normalized_name, canonical_name, volume_ml}
    """
    t = _clean_title(title)
    t = _BRACKET_DECO_RE.sub(" ", t)                                   # [NEW/시원촉촉], (신형)
    if brand:
        t = re.sub(re.escape(brand), " ", t, flags=re.IGNORECASE)      # 브랜드명 제거
    t = _COMMA_TRAIL_RE.sub("", t)                                     # ", SPF50+, 1개" 트레일링

    # 용량 추출 (제거 전에 먼저)
    vol_ml = 0.0
    m = _ML_RE.search(t) or _G_RE.search(t)
    if m:
        try:
            vol_ml = float(m.group(1))
        except (ValueError, TypeError):
            pass

    t = _ML_RE.sub(" ", t)
    t = _G_RE.sub(" ", t)
    t = _SPF_PA_RE.sub(" ", t)
    t = _UNIT_SINGLE_RE.sub(" ", t)
    t = _NON_WORD_RE.sub(" ", t)
    t = re.sub(r"\s{2,}", " ", t).strip()

    return {"normalized_name": t, "canonical_name": t, "volume_ml": vol_ml}


def infer_category_from_title(normalized_name: str, volume_ml: float = 0.0) -> str:
    """
    정규화된 제목에서 카테고리 추론.
    _CAT_KW_TABLE 키워드 우선, 용량 휴리스틱으로 보정.
    """
    nl = normalized_name.lower()
    for cat, kws in _CAT_KW_TABLE:
        for kw in kws:
            if kw in nl:
                return cat
    # 키워드 미식별 시 용량 기반 보정
    if 0 < volume_ml <= 35:
        return "선스틱"
    if volume_ml >= 80:
        return "선스프레이"
    return "선크림"


def infer_series_from_title(
    normalized_name: str,
    series_map: dict[str, str],
) -> "tuple[str, str]":
    """
    정규화된 제목에서 시리즈 추론.
    Returns: (series_name, confidence: 'high'|'medium'|'low')

    1순위: config known_series_map 키워드 매칭            → high
    2순위: 제품 유형 용어 제거 후 앞 2~3 단어로 추론       → medium
    최후:  "기타"                                          → low
    """
    # 1. 알려진 시리즈 우선 (긴 키워드 먼저 — "레이저 UV" > "UV")
    for kw in sorted(series_map.keys(), key=len, reverse=True):
        if kw in normalized_name:
            return series_map[kw], "high"

    # 2. 제품 유형 용어 제거 → 나머지로 신규 시리즈 추론
    stripped = normalized_name
    for term in _PROD_TYPE_TERMS:
        stripped = stripped.replace(term, " ")
    stripped = re.sub(r"\s{2,}", " ", stripped).strip()

    words = [w for w in stripped.split() if len(w) >= 2]
    if len(words) >= 2:
        return " ".join(words[:3]), "medium"
    if words and len(words[0]) >= 3:
        return words[0], "medium"

    return "기타", "low"


def merge_own_candidates(candidates: list[dict]) -> list[dict]:
    """
    동일 SKU 후보 병합.
    기준: (canonical_name, inferred_category, rounded_volume, inferred_series) 동일
    → raw_titles 리스트 병합, display_price = 최저 관찰가
    """
    bucket: dict[tuple, dict] = {}

    for c in candidates:
        canon   = c["canonical_name"]
        cat     = c["inferred_category"]
        vol     = c["volume_ml"]
        vol_key = round(vol / 5) * 5 if vol > 0 else 0   # 5ml 단위로 묶기
        series  = c["inferred_series"]
        key     = (canon.lower(), cat, vol_key, series)

        if key not in bucket:
            bucket[key] = {
                **c,
                "raw_titles":      [c["raw_title"]],
                "observed_prices": [c["price"]],
            }
        else:
            ex = bucket[key]
            if c["raw_title"] not in ex["raw_titles"]:
                ex["raw_titles"].append(c["raw_title"])
            ex["observed_prices"].append(c["price"])
            if c["price"] < ex["price"]:
                ex.update({"price": c["price"], "mall": c["mall"], "link": c["link"]})

    merged: list[dict] = []
    for entry in bucket.values():
        prices = sorted(entry["observed_prices"])
        mid    = len(prices) // 2
        entry["observed_price"]        = prices[0]
        entry["display_price"]         = prices[0]
        entry["official_price"]        = 0
        entry["median_observed_price"] = prices[mid]
        entry["source_count"]          = len(entry["raw_titles"])
        merged.append(entry)

    return merged


def collect_own_from_naver(
    brand: str,
    competitor_searches: list[dict],
    exclude: list[str],
    series_map: dict[str, str],
    mq_templates: dict,
    display: int = 40,
) -> "tuple[list[dict], dict]":
    """
    Layer 2 (대체): 3단계 파이프라인으로 자사 SKU 자동 발견.

    Stage 1: Raw discovery  — Naver API 브랜드×카테고리 검색
    Stage 2: Normalize      — 제목 정규화 + 카테고리/라인 자동 추론
    Stage 3: Merge          — 제목 변형 → 동일 SKU 후보 병합

    반환: (own_data: list[dict], sku_meta: dict)
    """
    url = "https://openapi.naver.com/v1/search/shop.json"

    # ── Stage 1: Raw discovery ──────────────────────────────────────────
    raw_items: list[dict] = []
    for search in competitor_searches:
        cat   = search.get("category", "")
        query = f"{brand} {search.get('query', cat)}"
        try:
            resp = requests.get(
                url, headers=NAVER_HEADERS(),
                params={"query": query, "display": display, "sort": "sim"},
                timeout=10,
            )
            if resp.status_code != 200:
                print(f"  [경고] 자사 발견 {resp.status_code} ({query})")
                continue
            for item in resp.json().get("items", []):
                title = _clean_title(item.get("title", ""))
                mall  = item.get("mallName", "")
                price = int(item.get("lprice") or 0)
                if _is_excluded(title, mall, exclude) or price == 0:
                    continue
                if brand not in title and brand not in mall:
                    continue
                if _score_pack(title)[0] >= 0.4:
                    continue
                if _score_title_noise(title)[0] >= 0.3:
                    continue
                if _PACK_ML_PLUS_RE.search(title):
                    continue
                raw_items.append({
                    "raw_title":  title,
                    "mall":       mall,
                    "price":      price,
                    "link":       item.get("link", ""),
                    "search_cat": cat,
                })
        except Exception as e:
            print(f"  [경고] 자사 발견 오류: {e}")

    # ── Stage 2: Normalize ──────────────────────────────────────────────
    normalized: list[dict] = []
    for raw in raw_items:
        norm   = normalize_product_title(raw["raw_title"], brand)
        cat    = infer_category_from_title(norm["canonical_name"], norm["volume_ml"])
        series, s_conf = infer_series_from_title(norm["canonical_name"], series_map)
        normalized.append({
            **norm,
            "raw_title":         raw["raw_title"],
            "mall":              raw["mall"],
            "price":             raw["price"],
            "link":              raw["link"],
            "inferred_category": cat,
            "inferred_series":   series,
            "series_confidence": s_conf,
        })

    # ── Stage 3: Merge ──────────────────────────────────────────────────
    merged_skus = merge_own_candidates(normalized)

    # ── own_data 변환 ────────────────────────────────────────────────────
    series_order = list(dict.fromkeys(series_map.values()))
    own_data: list[dict] = []

    for m in merged_skus:
        vol_ml  = m["volume_ml"]
        vol_str = f"{int(vol_ml)}ml" if vol_ml >= 1 else ""
        cat     = m["inferred_category"]
        series  = m["inferred_series"]
        name    = m["canonical_name"]
        mkt_q   = (mq_templates.get(cat) or [""])[0]

        entry = {
            "sku_id":            f"nv_{name[:30]}",
            "name":              name,
            "series":            series,
            "series_confidence": m["series_confidence"],
            "category":          cat,
            "subcategory":       "",
            "volume":            vol_str,
            "volume_ml":         vol_ml,
            # 가격 필드 분리 (official / observed / display)
            "official_price":    0,
            "observed_price":    m["observed_price"],
            "display_price":     m["display_price"],
            "our_price":         m["display_price"],      # 하위 호환
            "our_min_price":     m["observed_price"],
            "min_price":         m["observed_price"],
            "max_price":         max(m["observed_prices"]),
            "price_source":      "observed",
            # 발견 메타
            "raw_titles":        m["raw_titles"],
            "observed_prices":   m["observed_prices"],
            "source_count":      m["source_count"],
            "own_query":         f"{brand} {name[:40]}",
            "market_query":      mkt_q,
            "market_items":      [{"title": m["raw_titles"][0], "mall": m["mall"],
                                   "price": m["observed_price"], "link": m["link"],
                                   "is_ours": True}],
            "our_items":         [{"title": name, "mall": m["mall"],
                                   "price": m["observed_price"], "link": m["link"],
                                   "is_ours": True}],
            "own_query_hits":    m["source_count"],
            "total_hits":        m["source_count"],
            "has_data":          True,
        }
        entry["comp_status"] = {
            "code":  "ok",
            "label": "노출 정상",
            "desc":  f"Naver 발견 ({cat}) · {m['source_count']}개 변형 병합",
        }
        own_data.append(entry)

    own_data.sort(key=lambda x: (
        series_order.index(x["series"]) if x["series"] in series_order else 999,
        x["category"],
        x["name"],
    ))

    # ── 검증 로그 ────────────────────────────────────────────────────────
    n_raw    = len(raw_items)
    n_norm   = len(normalized)
    n_merged = len(own_data)
    n_high   = sum(1 for d in own_data if d.get("series_confidence") == "high")
    n_medium = sum(1 for d in own_data if d.get("series_confidence") == "medium")
    n_low    = sum(1 for d in own_data if d.get("series_confidence") == "low")
    n_other  = sum(1 for d in own_data if d.get("series") == "기타")

    cat_cnts:    dict[str, int] = {}
    series_cnts: dict[str, int] = {}
    for d in own_data:
        cat_cnts[d["category"]]  = cat_cnts.get(d["category"],  0) + 1
        series_cnts[d["series"]] = series_cnts.get(d["series"], 0) + 1

    print(f"  [own discovery] raw={n_raw} | normalized={n_norm} | merged={n_merged}")
    print(f"    series 신뢰도: high={n_high} medium={n_medium} low={n_low} | 기타={n_other}")
    print("  [카테고리]")
    for c, n in sorted(cat_cnts.items()):
        print(f"    {c}: {n}개")
    print("  [라인]")
    for s, n in sorted(series_cnts.items(), key=lambda x: -x[1]):
        print(f"    [{s}] {n}개")

    meta = {
        "source":        "naver_discovery",
        "file_path":     "Naver Shopping",
        "total_defined": n_merged,
        "active":        n_merged,
        "raw_found":     n_raw,
        "normalized":    n_norm,
        "series_high":   n_high,
        "series_medium": n_medium,
        "series_low":    n_low,
        "series_other":  n_other,
    }
    return own_data, meta


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 3 — SEED MARKET COLLECTION
# ══════════════════════════════════════════════════════════════════════════════

def collect_seed_market(
    category: str,
    keyword_templates: list[str],
    exclude: list[str],
    our_brand: str,
    display: int = 30,
) -> list[dict]:
    """
    Layer 3: 카테고리 일반 키워드 기반 시장 풀 수집.
    특정 브랜드 필터 없음 — 실제 검색 결과 그대로 반영.
    """
    url = "https://openapi.naver.com/v1/search/shop.json"
    raw: list[dict] = []

    for query in keyword_templates:
        params = {"query": query, "display": display}
        try:
            resp = requests.get(url, headers=NAVER_HEADERS(), params=params, timeout=10)
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    item["_source"]  = "seed"
                    item["_query"]   = query
                    item["_category"] = category
                    raw.append(item)
        except Exception:
            pass

    return raw


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 4 — BRAND MARKET COLLECTION
# ══════════════════════════════════════════════════════════════════════════════

def collect_brand_market(
    category: str,
    cat_keyword: str,
    brands: list[str],
    exclude: list[str],
    our_brand: str,
    display: int = 20,
) -> list[dict]:
    """
    Layer 4: 브랜드×카테고리 쿼리 기반 보강 수집.
    각 경쟁 브랜드를 명시적으로 검색해 누락 보완.
    """
    url = "https://openapi.naver.com/v1/search/shop.json"
    raw: list[dict] = []

    for brand in brands:
        query = f"{brand} {cat_keyword}"
        params = {"query": query, "display": display}
        try:
            resp = requests.get(url, headers=NAVER_HEADERS(), params=params, timeout=10)
            if resp.status_code == 200:
                for item in resp.json().get("items", []):
                    item["_source"]   = "brand"
                    item["_query"]    = query
                    item["_category"] = category
                    item["_searched_brand"] = brand
                    raw.append(item)
        except Exception:
            pass

    return raw


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 5 — MERGE + DEDUP
# ══════════════════════════════════════════════════════════════════════════════

def merge_market_candidates(
    seed: list[dict],
    brand: list[dict],
) -> list[dict]:
    """
    Layer 5: seed 우선 병합. brand는 중복 없는 신규 항목만 추가.
    dedup key: (title[:60].lower(), mall.lower())
    """
    seen:   set        = set()
    merged: list[dict] = []

    for item in seed:
        title = _clean_title(item.get("title", ""))
        mall  = item.get("mallName", "")
        key   = (title[:60].lower(), mall.lower())
        if key not in seen:
            seen.add(key)
            merged.append(item)

    for item in brand:
        title = _clean_title(item.get("title", ""))
        mall  = item.get("mallName", "")
        key   = (title[:60].lower(), mall.lower())
        if key not in seen:
            seen.add(key)
            merged.append(item)

    return merged


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 6 — SCORE-BASED CLASSIFICATION
# ══════════════════════════════════════════════════════════════════════════════

def _score_pack(title: str) -> tuple[float, list[str]]:
    """
    묶음/번들 시그널 점수 (0.0~1.0).
    높을수록 묶음 상품일 가능성 높음.
    """
    score   = 0.0
    reasons: list[str] = []

    m = _PACK_PLUS_RE.search(title)
    if m:
        a, b = int(m.group(1)), int(m.group(2))
        if 2 <= a + b <= 20:
            score = max(score, 0.85)
            reasons.append(f"N+M 패턴 ({a}+{b})")

    m = _PACK_COUNT_RE.search(title)
    if m:
        score = max(score, 0.90)
        reasons.append(f"개수 표기 ({m.group(1)}개)")

    m = _PACK_X_RE.search(title)
    if m:
        score = max(score, 0.85)
        reasons.append(f"곱셈 표기 x{m.group(1)}")

    if _PACK_KW_RE.search(title):
        score = max(score, 0.65)
        reasons.append("세트/기획/묶음 키워드")

    return score, reasons


def _score_volume(
    vol_ml: float | None,
    vol_min: float,
    vol_max: float,
) -> tuple[float, list[str]]:
    """
    카테고리 정상 용량 범위 적합도 (0.0~1.0).
    1.0 = 완벽한 적합, 0.0 = 범위 외.
    """
    if vol_ml is None:
        return 0.3, ["용량 정보 없음 (중립)"]
    if vol_min <= vol_ml <= vol_max:
        return 1.0, []
    # 경계 60% 이내
    if vol_ml < vol_min and vol_ml >= vol_min * 0.6:
        return 0.4, [f"용량 경계 하한 ({vol_ml:.0f}ml, 기준 {vol_min:.0f}ml)"]
    if vol_ml > vol_max and vol_ml <= vol_max * 1.5:
        return 0.4, [f"용량 경계 상한 ({vol_ml:.0f}ml, 기준 {vol_max:.0f}ml)"]
    return 0.0, [f"용량 범위 외 ({vol_ml:.0f}ml, 기준 {vol_min:.0f}-{vol_max:.0f}ml)"]


def _score_price(
    price: int,
    vol_ml: float | None,
    our_avg: float,
    cat_median_ml: float,
) -> tuple[float, list[str]]:
    """
    가격 정상성 점수 (-1.0~1.0).
    -1.0 = 심각 이상치, 0.5 = 정상, 1.0 = 고가.
    dual reference: 자사 공식가 + 카테고리 ml 중앙값.
    """
    score   = 0.5
    reasons: list[str] = []

    # A. 자사 공식가 기준
    if our_avg > 0 and price > 0:
        ratio = price / our_avg
        if ratio < 0.35:
            score = min(score, -0.9)
            reasons.append(f"공식가 {ratio:.0%} — 심각 저가")
        elif ratio < 0.50:
            score = min(score, -0.5)
            reasons.append(f"공식가 {ratio:.0%} — 이상치 의심")

    # B. 카테고리 ml당 중앙값 기준
    if cat_median_ml > 0 and vol_ml and vol_ml > 0 and price > 0:
        item_ml = price / vol_ml
        ml_ratio = item_ml / cat_median_ml
        if ml_ratio < 0.30:
            score = min(score, -0.8)
            reasons.append(f"ml가 {item_ml:.1f}원 (중앙값 {cat_median_ml:.1f}원 대비 {ml_ratio:.0%})")
        elif ml_ratio < 0.50:
            score = min(score, -0.4)
            reasons.append(f"ml가 낮음 (중앙값 대비 {ml_ratio:.0%})")

    return score, reasons


def _score_title_noise(title: str) -> tuple[float, list[str]]:
    """
    프로모션/증정/리뉴얼 등 단품 아님 시그널 (0.0~1.0).
    높을수록 일반 단품이 아닐 가능성.
    """
    score   = 0.0
    reasons: list[str] = []

    if _NOISE_PROMO_RE.search(title):
        score = max(score, 0.40)
        reasons.append("증정/이벤트/특가 키워드")

    if _NOISE_MISC_RE.search(title):
        score = max(score, 0.30)
        reasons.append("대용량/리뉴얼/리필 키워드")

    return score, reasons


def classify_product(item: dict, context: dict) -> dict:
    """
    Layer 6: 점수 기반 상품 분류.

    context:
      vol_min, vol_max     float  — 카테고리 정상 용량 범위
      our_avg              float  — 자사 카테고리 평균가
      cat_median_ml        float  — 1차 분류 후 단품 ml 중앙값

    Returns:
      type        single | promo | bundle | abnormal | unknown
      confidence  high | medium | low
      scores      {pack, volume, price, noise}
      reasons     list[str]
    """
    title   = item.get("title", "")
    price   = item.get("price", 0) or 0
    vol_ml  = item.get("vol_ml")      # orchestrator가 미리 계산
    if vol_ml is None:
        vol_ml = _parse_volume_ml(title)

    vol_min       = context.get("vol_min", 0)
    vol_max       = context.get("vol_max", 9999)
    our_avg       = context.get("our_avg", 0)
    cat_median_ml = context.get("cat_median_ml", 0)

    pack_score,   pack_r   = _score_pack(title)
    volume_score, volume_r = _score_volume(vol_ml, vol_min, vol_max)
    price_score,  price_r  = _score_price(price, vol_ml, our_avg, cat_median_ml)
    noise_score,  noise_r  = _score_title_noise(title)

    reasons = pack_r + volume_r + price_r + noise_r
    scores  = {
        "pack":   round(pack_score,   2),
        "volume": round(volume_score, 2),
        "price":  round(price_score,  2),
        "noise":  round(noise_score,  2),
    }

    # ── 분류 규칙 (우선순위 순) ──
    # 1. 비정상 (가격 심각 이탈)
    if price_score <= -0.7:
        conf = "high" if price_score <= -0.9 else "medium"
        return {"type": "abnormal", "confidence": conf,
                "scores": scores, "reasons": reasons + ["가격 이상치 => abnormal"]}

    # 2. 번들 (강한 묶음 시그널)
    if pack_score >= 0.7:
        conf = "high" if pack_score >= 0.85 else "medium"
        return {"type": "bundle", "confidence": conf,
                "scores": scores, "reasons": reasons}

    # 3. 프로모 (번들/행사 시그널)
    if pack_score >= 0.4 or noise_score >= 0.5:
        conf = "medium" if pack_score < 0.6 else "high"
        return {"type": "promo", "confidence": conf,
                "scores": scores, "reasons": reasons}

    # 4. 단품 (용량 OK + 가격 정상 + 노이즈 없음)
    if volume_score >= 0.4 and price_score >= -0.3 and noise_score < 0.3 and pack_score < 0.4:
        conf = "high" if (volume_score >= 0.8 and noise_score < 0.1) else "medium"
        return {"type": "single", "confidence": conf,
                "scores": scores, "reasons": reasons}

    # 5. 판단 불가 (unknown)
    return {"type": "unknown", "confidence": "low",
            "scores": scores, "reasons": reasons + ["분류 기준 미충족 => unknown"]}


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 7 — OUTLIER DETECTION
# ══════════════════════════════════════════════════════════════════════════════

def detect_price_outlier(item: dict, category_context: dict) -> dict:
    """
    Layer 7: 3단 이상치 탐지.

    category_context:
      abs_min           float  — 절대 최저가 (config)
      singles_ml_p25    float  — 단품 ml가 25퍼센타일
      our_avg           float  — 자사 카테고리 평균가
      ml_p25_threshold  float  — default 0.5
      own_ref_threshold float  — default 0.4

    Rules:
      Tier A  abs_price : price < abs_min
      Tier B  ml_p25    : ml_price < singles_ml_p25 * threshold
      Tier C  own_ref   : price < our_avg * threshold

    2+ Tier 충족 -> confidence=high -> abnormal
    1  Tier 충족 -> confidence=low  -> unknown (단품에서 강제 abnormal X)
    """
    price    = item.get("price", 0) or 0
    ml_price = item.get("ml_price")

    abs_min      = category_context.get("abs_min", 0)
    ml_p25       = category_context.get("singles_ml_p25", 0)
    our_avg      = category_context.get("our_avg", 0)
    ml_thresh    = category_context.get("ml_p25_threshold",  0.5)
    own_thresh   = category_context.get("own_ref_threshold", 0.4)
    n_singles    = category_context.get("n_singles", 0)

    triggered: list[str] = []

    # Tier A: 절대 최저가
    if abs_min > 0 and price > 0 and price < abs_min:
        triggered.append(f"abs_price: {price:,}원 < 최저 {abs_min:,}원")

    # Tier B: ml당 P25 기준 (n>=4 이상일 때만 유의미)
    if ml_price and ml_p25 > 0 and n_singles >= 4:
        if ml_price < ml_p25 * ml_thresh:
            triggered.append(
                f"ml_p25: {ml_price:.1f}원/ml < P25({ml_p25:.1f}) * {ml_thresh}"
            )

    # Tier C: 자사 공식가 기준
    if our_avg > 0 and price > 0 and price < our_avg * own_thresh:
        triggered.append(
            f"own_ref: {price:,}원 < 공식가기준 {our_avg * own_thresh:,.0f}원"
        )

    if not triggered:
        return {"is_outlier": False, "confidence": "none",
                "triggered": [], "reason": "", "rule": "clean"}

    n = len(triggered)
    confidence = "high" if n >= 2 else "low"
    rule       = "multi_tier_abnormal" if n >= 2 else "single_tier_unknown"
    reason     = " & ".join(triggered)

    return {"is_outlier": True, "confidence": confidence,
            "triggered": triggered, "reason": reason, "rule": rule}


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 8 — REFERENCE PRICE BUILDING
# ══════════════════════════════════════════════════════════════════════════════

def build_category_price_reference(
    single_items: list[dict],
    sku_ml: float,
    min_samples: int = 5,
) -> dict:
    """
    Layer 8: 카테고리 단위 참조가 계산.
    primary: median (이상치에 강건)
    secondary: avg, p25

    Returns:
      avg_ml, median_ml, p25_ml      float  — ml당 가격
      avg_ref, median_ref, p25_ref   int    — sku_ml 기준 참조가
      sample_count, enough_data, reliability
    """
    ml_prices = sorted(
        it["ml_price"] for it in single_items if it.get("ml_price") and it["ml_price"] > 0
    )
    n = len(ml_prices)
    rel = _reliability_tier(n)

    if n == 0:
        return {
            "avg_ml": 0, "median_ml": 0, "p25_ml": 0,
            "avg_ref": 0, "median_ref": 0, "p25_ref": 0,
            "sample_count": 0, "enough_data": False,
            "reliability": rel,
        }

    avg_ml    = sum(ml_prices) / n
    mid       = n // 2
    median_ml = ml_prices[mid] if n % 2 else (ml_prices[mid - 1] + ml_prices[mid]) / 2
    p25_ml    = ml_prices[max(0, n // 4)]

    def ref(ml): return round(ml * sku_ml) if (ml > 0 and sku_ml > 0) else 0

    return {
        "avg_ml":    round(avg_ml, 1),
        "median_ml": round(median_ml, 1),
        "p25_ml":    round(p25_ml, 1),
        "avg_ref":    ref(avg_ml),
        "median_ref": ref(median_ml),
        "p25_ref":    ref(p25_ml),
        "sample_count": n,
        "enough_data":  rel["enough"],
        "reliability":  rel,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 공통 유틸리티 헬퍼
# ══════════════════════════════════════════════════════════════════════════════

def _clean_title(title: str) -> str:
    return title.replace("<b>", "").replace("</b>", "").strip()


def _is_excluded(text: str, mall: str, exclude: list[str]) -> bool:
    return any(kw.lower() in text.lower() or kw.lower() in mall.lower()
               for kw in exclude)


def _complete_points(pts: list) -> list:
    """is_partial 아닌(=완전한) 데이터 포인트만 반환."""
    return [p for p in pts if not p.get("is_partial")]


def _median(nums: list) -> float:
    if not nums:
        return 0.0
    s = sorted(nums)
    n = len(s)
    return s[n // 2] if n % 2 else (s[n // 2 - 1] + s[n // 2]) / 2


def _percentile(nums_sorted: list, pct: float) -> float:
    """0~1 백분위 반환. nums_sorted는 오름차순 정렬된 리스트."""
    if not nums_sorted:
        return 0.0
    idx = int(len(nums_sorted) * pct)
    return nums_sorted[min(idx, len(nums_sorted) - 1)]


def _parse_volume_ml(title: str) -> float | None:
    m = _ML_RE.search(title)
    if m:
        try:
            return float(m.group(1))
        except Exception:
            pass
    return None


def _total_volume_ml(title: str, count: int) -> float | None:
    ml_vals = [float(v) for v in _ML_RE.findall(title)]
    if not ml_vals:
        g_vals = [float(v) for v in _G_RE.findall(title)]
        if not g_vals:
            return None
        primary = max((v for v in g_vals if 5 <= v <= 500), default=g_vals[0])
        return primary * count
    primary = max((v for v in ml_vals if 10 <= v <= 500), default=ml_vals[0])
    return primary * count


def _reliability_tier(n: int) -> dict:
    if n >= 10:
        return {"tier": "high",   "label": "High",   "color": "green", "enough": True}
    if n >= 5:
        return {"tier": "medium", "label": "Medium", "color": "amber", "enough": True}
    return     {"tier": "low",    "label": "Low",    "color": "red",   "enough": False}


def _bucket_summary(items: list[dict], min_samples: int = 5) -> dict:
    prices    = [i["price"]    for i in items]
    ml_prices = [i["ml_price"] for i in items if i.get("ml_price")]
    brands    = {(i["brand"] if i.get("brand") and i["brand"] != "-"
                  else i.get("mall", "-")) for i in items}
    n         = len(items)
    rel       = _reliability_tier(n)
    enough    = rel["enough"]
    return {
        "brand_count":      len(brands),
        "sample_count":     n,
        "avg_price":        int(sum(prices) / len(prices)) if prices and enough else 0,
        "median_price":     int(_median(prices))           if prices and enough else 0,
        "avg_ml_price":     round(sum(ml_prices) / len(ml_prices), 1) if ml_prices and enough else 0,
        "median_ml_price":  round(_median(ml_prices), 1)              if ml_prices and enough else 0,
        "reliability":      rel,
        "enough_data":      enough,
    }


def _our_category_avg(own_data: list[dict], category: str) -> float:
    prices = [int(p.get("our_price") or 0) for p in (own_data or [])
              if p.get("category") == category and (p.get("our_price") or 0) > 0]
    if not prices:
        prices = [int(p.get("our_price") or 0) for p in (own_data or [])
                  if (p.get("our_price") or 0) > 0]
    return sum(prices) / len(prices) if prices else 0.0


def _our_category_ml_price(own_data: list[dict], category: str) -> float:
    vals = []
    for p in own_data or []:
        if p.get("category") != category:
            continue
        price  = p.get("our_price") or 0
        vol_ml = p.get("volume_ml") or 0
        if not vol_ml:
            vol_str = str(p.get("volume") or "")
            m = re.search(r"(\d+(?:\.\d+)?)", vol_str)
            vol_ml = float(m.group(1)) if m else 0
        if vol_ml > 0 and price > 0:
            vals.append(price / vol_ml)
    return round(sum(vals) / len(vals), 1) if vals else 0.0


def _match_brand(title: str, brand_field: str, brand_list: list[str]) -> str | None:
    tl = title.lower()
    bl = brand_field.lower()
    for b in brand_list:
        if b.lower() in tl or b.lower() in bl:
            return b
    return None


def _build_alias_map(aliases: list[list[str]]) -> dict[str, str]:
    m: dict[str, str] = {}
    for group in aliases:
        if not group:
            continue
        canonical = group[0]
        for alias in group:
            m[alias.lower()] = canonical
    return m


def _canonical_brand(matched: str, alias_map: dict[str, str]) -> str:
    return alias_map.get(matched.lower(), matched)


def _own_sku_coverage(own_data: list[dict]) -> dict:
    total   = len(own_data)
    matched = sum(1 for p in own_data if p.get("has_data"))
    unmatched_skus = [
        {"name": p.get("name", ""), "category": p.get("category", "")}
        for p in own_data if not p.get("has_data")
    ]
    return {
        "total":          total,
        "matched":        matched,
        "unmatched":      total - matched,
        "unmatched_skus": unmatched_skus,
    }


def _sku_status(sku_result: dict, market_median_ml: float) -> dict:
    """SKU 단위 상태 판단. 시장 ml당 중앙가 대비 괴리율 기반."""
    price  = sku_result.get("official_price") or sku_result.get("our_price") or 0
    vol_ml = sku_result.get("volume_ml") or 0
    if not vol_ml:
        vol_str = str(sku_result.get("volume") or "")
        vm = re.search(r"(\d+(?:\.\d+)?)", vol_str)
        vol_ml = float(vm.group(1)) if vm else 0
    our_ml = (price / vol_ml) if (vol_ml > 0 and price > 0) else 0.0

    if not sku_result.get("has_data"):
        return {"status": "warn", "icon": "🟡",
                "reason": "시장 미노출 — 검색 결과 없음",
                "action": "own_query 키워드 개선 후 재확인"}
    if market_median_ml <= 0 or our_ml <= 0:
        return {"status": "ok", "icon": "🟢", "reason": "가격 비교 불가", "action": ""}

    diff = round((our_ml - market_median_ml) / market_median_ml * 100)
    if diff >= 50:
        return {"status": "danger", "icon": "🔴",
                "reason": f"가격 괴리 {diff:+}% — 경쟁력 낮음",
                "action": "가격 전략 재검토 / 행사 SKU 기획"}
    if diff <= -50:
        return {"status": "danger", "icon": "🔴",
                "reason": f"가격 {diff:+}% — 가격 붕괴 의심",
                "action": "유통 채널 덤핑 여부 점검"}
    if diff >= 20:
        return {"status": "warn", "icon": "🟡",
                "reason": f"시장 중앙값 대비 {diff:+}% 고가",
                "action": "프리미엄 메시지 강화 또는 가격 조정 검토"}
    return {"status": "ok", "icon": "🟢",
            "reason": f"시장 중앙값 근접 ({diff:+}%)", "action": ""}


# ══════════════════════════════════════════════════════════════════════════════
# 경쟁사 인사이트 생성기
# ══════════════════════════════════════════════════════════════════════════════

def _generate_comp_insight(comp: dict, brand_name: str) -> dict:
    """카테고리별 시장 분석 인사이트 — 4단 구조."""
    cat         = comp.get("category", "")
    ssum        = comp.get("single",   {}).get("summary") or {}
    psum        = comp.get("promo",    {}).get("summary") or {}
    asum        = comp.get("abnormal", {}).get("summary") or {}
    unksum      = comp.get("unknown",  {}).get("summary") or {}
    sn          = ssum.get("sample_count", 0)
    pn          = psum.get("sample_count", 0)
    an          = asum.get("sample_count", 0)
    ukn         = unksum.get("sample_count", 0)
    our_ml      = comp.get("our_ml_price") or 0

    # price_ref 우선 사용, 없으면 summary 기반
    price_ref   = comp.get("price_ref") or {}
    market_ml   = price_ref.get("median_ml") or ssum.get("median_ml_price") or ssum.get("avg_ml_price") or 0
    enough      = ssum.get("enough_data", False)
    reliability = (ssum.get("reliability") or {}).get("tier", "low")
    total       = (sn + pn + an + ukn) or 1

    s_ratio = round(sn / total * 100)
    p_ratio = round(pn / total * 100)
    a_ratio = round(an / total * 100)
    u_ratio = round(ukn / total * 100)

    # 1. 시장 구조 문장
    market_lines = [f"단품 {s_ratio}% / 행사 {p_ratio}% / 비정상 {a_ratio}%"]
    if u_ratio > 10:
        market_lines.append(f"미분류 {u_ratio}% (추가 검토 필요)")
    if p_ratio > 30:
        market_lines.append("할인 중심 시장")
    elif s_ratio > 60:
        market_lines.append("단품 중심 안정 시장")
    market_ml_p = psum.get("avg_ml_price") or 0
    if market_ml > 0 and market_ml_p > 0:
        gap = round((market_ml_p - market_ml) / market_ml * 100)
        if gap <= -40:
            market_lines.append(f"행사가 단품 대비 {gap:+}% => 가격 경쟁 심화")
    if a_ratio > 20:
        market_lines.append("가격 붕괴 리스크 존재")
    market_structure = " · ".join(market_lines)

    # 2. 자사 포지션
    diff_pct: int | None = None
    if our_ml > 0 and market_ml > 0 and enough:
        diff_pct = round((our_ml - market_ml) / market_ml * 100)
        if diff_pct >= 50:
            position_label = "프리미엄 포지션"
        elif diff_pct >= 20:
            position_label = "고가 포지션"
        elif diff_pct >= -20:
            position_label = "시장 평균"
        else:
            position_label = "저가 포지션"
        comp_strength = "높음" if diff_pct <= 0 else "낮음"
        position_sentence = (
            f"자사는 시장 중앙값 대비 {diff_pct:+}% / 가격 경쟁력: {comp_strength}"
        )
    else:
        position_label    = "데이터 부족"
        position_sentence = "단품 시장 데이터 불충분 — 비교 불가"

    # 3. 문제 + 액션
    problems: list[str] = []
    actions:  list[str] = []

    if not enough:
        problems.append(f"단품 표본 {sn}개 — 신뢰도 낮음")
        actions.append("동일 용량 경쟁 SKU 확인 후 재수집 필요")
    elif reliability == "medium":
        problems.append(f"단품 표본 {sn}개 — 중간 신뢰도")
        actions.append("표본 수 확보를 위해 추가 브랜드 검색 검토")

    if diff_pct is not None:
        if diff_pct >= 50:
            problems.append(f"자사 {diff_pct:+}% 프리미엄 — 가격 경쟁력 부족")
            actions.append("중간 가격 SKU 추가 또는 행사 SKU 기획 검토")
        elif diff_pct >= 20:
            problems.append(f"자사 {diff_pct:+}% 소폭 고가 — 프리미엄 포지션")
            actions.append("제품 차별점 메시지 강화로 프리미엄 정당화")
        elif diff_pct <= -20:
            problems.append(f"자사 {diff_pct:+}% 저가 — 가격 인상 또는 번들 검토")
            actions.append("번들/용량 업 전략으로 객단가 개선 검토")
        else:
            problems.append("자사 시장 평균 근접 — 차별화 전략 필요")
            actions.append("제품 성분·기능 강조 마케팅으로 차별화")

    if pn > sn and sn > 0:
        problems.append(f"행사({pn}개) > 단품({sn}개) — 정상 가격 경쟁 어려움")
        actions.append("행사 SKU 전략적 운영 검토")
    elif p_ratio >= 50:
        problems.append(f"행사·묶음 {p_ratio}% — 단품 수요 낮은 구간")
        actions.append("행사 중심 대응: 증정 또는 번들 SKU 기획 검토")

    if a_ratio > 20:
        problems.append(f"비정상 가격 {a_ratio}% — 유통 리스크")
        actions.append("비정상 판매 채널 점검 / 병행 덤핑 여부 확인")
    elif an >= 3:
        problems.append(f"비정상 가격 상품 {an}개 — 병행/비정품 점검")
        actions.append("비정상 판매 채널 모니터링")

    # 4. Priority score
    price_gap_norm   = min(abs(diff_pct or 0), 100)
    market_share_inv = 100 - s_ratio
    abnormal_norm    = min(a_ratio, 100)
    priority_score   = round(
        price_gap_norm * 0.4 + market_share_inv * 0.3 + abnormal_norm * 0.3, 1
    )

    scored_actions = [{"problem": p, "action": a, "score": priority_score}
                      for p, a in zip(problems, actions)]
    scored_actions.sort(key=lambda x: x["score"], reverse=True)

    return {
        "category":              cat,
        "problems":              problems,
        "actions":               actions,
        "market_structure":      market_structure,
        "position_label":        position_label,
        "position_sentence":     position_sentence,
        "diff_pct":              diff_pct,
        "s_ratio":               s_ratio,
        "p_ratio":               p_ratio,
        "a_ratio":               a_ratio,
        "u_ratio":               u_ratio,
        "priority_score":        priority_score,
        "high_priority_actions": scored_actions[:3],
        "position_pct":          diff_pct,
        "our_ml":                our_ml,
        "market_ml":             market_ml,
        "enough":                enough,
        "reliability":           reliability,
        "sn": sn, "pn": pn, "an": an,
    }


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 11 — COMPETITOR MARKET ORCHESTRATOR
# ══════════════════════════════════════════════════════════════════════════════

def build_competitor_market(
    comp_searches: list[dict],
    config: dict,
    own_data: list[dict],
    top_n: int = 5,
) -> list[dict]:
    """
    Layer 11: 카테고리별 경쟁사 시장 완전 파이프라인.

    per category:
      Stage 1  collect_seed_market()   — 일반 키워드 수집
      Stage 2  collect_brand_market()  — 브랜드 보강 수집
      Stage 3  merge_market_candidates() — 병합/중복제거
      Stage 4  classify_product()      — 점수 기반 분류
      Stage 5  detect_price_outlier()  — 3단 이상치 필터
      Stage 6  재수집 (단품 < min_single)
      Stage 7  build_category_price_reference() — 참조가
      Stage 8  요약 + 로그
    """
    results    = []
    our_brand  = config.get("our_brand_keyword", "")
    exclude    = config.get("exclude_keywords", [])
    comp_brands = config.get("competitor_brands", [])
    ex_brands  = config.get("exclude_brands", [])
    vmap       = config.get("category_volume_ranges", {})
    mq_tmpl    = config.get("market_query_templates", {})
    price_rules = config.get("category_price_rules", {})
    ab_rules   = config.get("abnormal_rules", {})
    aliases    = config.get("competitor_brand_aliases", [])
    amap       = _build_alias_map(aliases)
    min_single = config.get("single_min_samples", 5)
    ex_brands_lower = [b.lower() for b in (ex_brands or [])]

    own_ref_thresh = float(ab_rules.get("own_ref_threshold", 0.4))
    ml_p25_thresh  = float(ab_rules.get("ml_p25_threshold",  0.5))

    for search in comp_searches:
        cat_keyword = search.get("query", "")
        category    = search.get("category", "")

        vrange  = vmap.get(category, {})
        vol_min = float(vrange.get("min_ml", 10))
        vol_max = float(vrange.get("max_ml", 500))
        our_avg = _our_category_avg(own_data, category)
        our_ml  = _our_category_ml_price(own_data, category)
        abs_min = float((price_rules.get(category) or {}).get("abs_min", 0))

        local_brands     = [b.strip() for b in (search.get("competitor_brands") or []) if b.strip()]
        effective_brands = local_brands or comp_brands

        # ── Stage 1: Seed market ──────────────────────────────────────
        seed_templates = mq_tmpl.get(category, [cat_keyword])
        seed_raw = collect_seed_market(category, seed_templates, exclude, our_brand, display=30)

        # ── Stage 2: Brand enrichment ─────────────────────────────────
        brand_raw = collect_brand_market(
            category, cat_keyword, effective_brands, exclude, our_brand, display=20
        )

        # ── Stage 3: Merge ────────────────────────────────────────────
        merged = merge_market_candidates(seed_raw, brand_raw)

        # ── Stage 4: Filter + Classify (1차, cat_median_ml=0) ─────────
        context = {
            "vol_min":       vol_min,
            "vol_max":       vol_max,
            "our_avg":       our_avg,
            "cat_median_ml": 0,   # 1차: unknown으로 채워짐
        }

        buckets: dict[str, list] = {
            "single": [], "promo": [], "bundle": [],
            "abnormal": [], "unknown": [],
        }
        dedup_seen: set = set()
        dedup_count = 0
        brand_filter_count = 0

        for item in merged:
            title      = _clean_title(item.get("title", ""))
            mall       = item.get("mallName", "")
            brand_field = (item.get("brand") or "").strip()
            price      = int(item.get("lprice") or 0)

            if price <= 0:
                continue
            if our_brand and (our_brand in title or our_brand in mall or our_brand in brand_field):
                continue
            if _is_excluded(title, mall, exclude):
                continue
            bl = brand_field.lower()
            if any(eb in bl or eb in title.lower() for eb in ex_brands_lower):
                continue

            matched_raw = _match_brand(title, brand_field, effective_brands) if effective_brands else None
            if effective_brands and matched_raw is None:
                continue
            brand_filter_count += 1

            key = (title[:60].lower(), mall.lower())
            if key in dedup_seen:
                continue
            dedup_seen.add(key)
            dedup_count += 1

            matched_brand = _canonical_brand(matched_raw, amap) if matched_raw else None
            vol_ml        = _parse_volume_ml(title)
            total_ml      = _total_volume_ml(title, 1)
            ml_price      = round(price / total_ml, 1) if total_ml and total_ml > 0 else None

            entry = {
                "title":    title,
                "mall":     mall,
                "price":    price,
                "brand":    matched_brand or brand_field or mall or "-",
                "link":     item.get("link", ""),
                "vol_ml":   vol_ml,
                "total_ml": total_ml,
                "ml_price": ml_price,
                "source":   item.get("_source", ""),
            }

            result = classify_product(entry, context)
            entry["type"]             = result["type"]
            entry["confidence"]       = result["confidence"]
            entry["classify_scores"]  = result["scores"]
            entry["classify_reasons"] = result["reasons"]

            # 단품은 용량 범위도 검사
            if result["type"] == "single":
                if vol_ml is not None and not (vol_min <= vol_ml <= vol_max):
                    entry["type"] = "single_out"

            t = entry["type"]
            if t in buckets:
                buckets[t].append(entry)
            # single_out은 버킷에 넣지 않음 (dropped)

        # ── 1차 단품 ml 중앙값으로 context 업데이트 + 2차 분류 ─────────
        first_singles = buckets["single"][:]
        ml_vals_1st   = sorted(e["ml_price"] for e in first_singles if e.get("ml_price"))
        cat_median_1st = _median(ml_vals_1st) if ml_vals_1st else 0
        context["cat_median_ml"] = cat_median_1st

        # unknown 재분류
        reclassified: list[dict] = []
        for entry in buckets["unknown"][:]:
            result2 = classify_product(entry, context)
            if result2["type"] != "unknown":
                entry["type"]       = result2["type"]
                entry["confidence"] = result2["confidence"]
                if result2["type"] == "single":
                    vol_ml = entry.get("vol_ml")
                    if vol_ml is not None and not (vol_min <= vol_ml <= vol_max):
                        entry["type"] = "single_out"
                        continue
                buckets[result2["type"]].append(entry)
                reclassified.append(entry)
        for e in reclassified:
            if e in buckets["unknown"]:
                buckets["unknown"].remove(e)

        # ── Stage 5: 이상치 후처리 ───────────────────────────────────
        ml_sorted_singles = sorted(
            e["ml_price"] for e in buckets["single"] if e.get("ml_price")
        )
        n_singles = len(ml_sorted_singles)
        ml_p25    = _percentile(ml_sorted_singles, 0.25) if n_singles >= 4 else 0

        cat_ctx = {
            "abs_min":           abs_min,
            "singles_ml_p25":    ml_p25,
            "our_avg":           our_avg,
            "ml_p25_threshold":  ml_p25_thresh,
            "own_ref_threshold": own_ref_thresh,
            "n_singles":         n_singles,
        }

        single_before_outlier = len(buckets["single"])
        keep_singles: list[dict] = []
        for it in buckets["single"]:
            outlier = detect_price_outlier(it, cat_ctx)
            if outlier["is_outlier"]:
                it["outlier_reason"] = outlier["reason"]
                it["outlier_rule"]   = outlier["rule"]
                if outlier["confidence"] == "high":
                    it["type"]        = "abnormal"
                    it["confidence"]  = "low"
                    buckets["abnormal"].append(it)
                else:
                    # 1-tier: unknown으로 (강제 abnormal X)
                    it["type"]       = "unknown"
                    it["confidence"] = "low"
                    buckets["unknown"].append(it)
            else:
                keep_singles.append(it)

        buckets["single"] = keep_singles
        outlier_moved     = single_before_outlier - len(buckets["single"])

        # ── Stage 6: 단품 부족 시 재수집 ────────────────────────────
        if len(buckets["single"]) < min_single:
            extra_raw = collect_brand_market(
                category, cat_keyword, effective_brands, exclude, our_brand, display=40
            )
            extra = merge_market_candidates([], extra_raw)
            for item in extra:
                title      = _clean_title(item.get("title", ""))
                mall       = item.get("mallName", "")
                brand_field = (item.get("brand") or "").strip()
                price      = int(item.get("lprice") or 0)
                if price <= 0: continue
                mr = _match_brand(title, brand_field, effective_brands) if effective_brands else None
                if effective_brands and mr is None: continue
                key = (title[:60].lower(), mall.lower())
                if key in dedup_seen: continue
                dedup_seen.add(key)

                vol_ml   = _parse_volume_ml(title)
                total_ml = _total_volume_ml(title, 1)
                ml_price = round(price / total_ml, 1) if total_ml else None
                entry = {
                    "title": title, "mall": mall, "price": price,
                    "brand": _canonical_brand(mr, amap) if mr else brand_field or mall or "-",
                    "link": item.get("link", ""), "vol_ml": vol_ml,
                    "total_ml": total_ml, "ml_price": ml_price,
                    "source": "retry",
                }
                res = classify_product(entry, context)
                entry["type"]       = res["type"]
                entry["confidence"] = res["confidence"]
                if res["type"] == "single":
                    if vol_ml is not None and not (vol_min <= vol_ml <= vol_max):
                        continue
                    ol = detect_price_outlier(entry, cat_ctx)
                    if not ol["is_outlier"] or ol["confidence"] == "low":
                        buckets["single"].append(entry)
                elif res["type"] in ("promo", "bundle"):
                    buckets[res["type"]].append(entry)

            print(f"  [{category}] 재수집 후 단품: {len(buckets['single'])}개")

        # ── Stage 7: 참조가 계산 ─────────────────────────────────────
        # 자사 SKU 대표 용량 (카테고리 평균)
        own_vols = [p.get("volume_ml") or 0 for p in own_data if p.get("category") == category]
        own_vols = [v for v in own_vols if v > 0]
        ref_sku_ml = sum(own_vols) / len(own_vols) if own_vols else 0

        price_ref = build_category_price_reference(
            buckets["single"], sku_ml=ref_sku_ml, min_samples=min_single
        )

        promo_all    = buckets["promo"] + buckets["bundle"]
        single_sum   = _bucket_summary(buckets["single"],  min_samples=min_single)
        promo_sum    = _bucket_summary(promo_all,           min_samples=1)
        abnormal_sum = _bucket_summary(buckets["abnormal"], min_samples=1)
        unknown_sum  = _bucket_summary(buckets["unknown"],  min_samples=1)

        # 포지션 판단 (median 기준)
        our_position = "-"
        med_ml = price_ref.get("median_ml", 0)
        if our_ml > 0 and med_ml > 0 and single_sum.get("enough_data"):
            ratio = (our_ml - med_ml) / med_ml
            our_position = (
                "자사가 저가" if ratio <= -0.10 else
                "유사 수준"   if ratio <=  0.10 else
                "자사가 고가"
            )

        # 브랜드 포지셔닝 데이터
        brand_ml: dict[str, list] = {}
        for it in buckets["single"]:
            if it.get("ml_price"):
                brand_ml.setdefault(it["brand"], []).append(it["ml_price"])
        brand_data = [
            {"brand": b, "avg_ml_price": round(sum(vs) / len(vs), 1),
             "count": len(vs), "is_ours": False}
            for b, vs in sorted(brand_ml.items(), key=lambda x: sum(x[1]) / len(x[1]))
        ]
        if our_ml > 0:
            brand_data.append({"brand": our_brand or "자사",
                                "avg_ml_price": our_ml, "count": 0, "is_ours": True})

        found_brands = {it["brand"] for it in buckets["single"] + promo_all}
        coverage = {
            "requested": len(effective_brands),
            "found": len([b for b in effective_brands
                          if any(b.lower() in fb.lower() for fb in found_brands)]),
        }

        # ── Stage 8: 로그 ─────────────────────────────────────────────
        sn  = len(buckets["single"])
        pn  = len(promo_all)
        an  = len(buckets["abnormal"])
        ukn = len(buckets["unknown"])
        print(
            f"  [{category}] "
            f"seed={len(seed_raw)} brand={len(brand_raw)} "
            f"merged={len(merged)} dedup={dedup_count} | "
            f"single={sn} promo/bundle={pn} abnormal={an} unknown={ukn} "
            f"outlier={outlier_moved} | "
            f"ref_ml(med)={price_ref.get('median_ml', 0):.1f}원/ml"
        )

        # 자사 SKU별 로그
        for p in own_data:
            if p.get("category") == category:
                print(
                    f"    SKU [{p.get('name','')[:20]}] "
                    f"own_hits={p.get('own_query_hits', '-')} "
                    f"vol={p.get('volume_ml', 0):.0f}ml "
                    f"ref_price={price_ref.get('median_ref', 0):,}원"
                )

        results.append({
            "category":        category,
            "query":           cat_keyword,
            "criteria":        (
                f"시드 {len(seed_templates)}개 키워드 + {len(effective_brands)}개 브랜드 · "
                f"단품 {vol_min:.0f}~{vol_max:.0f}ml 기준"
            ),
            "our_avg":         int(our_avg) if our_avg else 0,
            "our_ml_price":    our_ml,
            "our_position":    our_position,
            "vol_range":       {"min": vol_min, "max": vol_max},
            "single_out_cnt":  single_before_outlier - sn,
            "brand_data":      brand_data,
            "coverage":        coverage,
            "price_ref":       price_ref,
            "single":   {"summary": single_sum,   "items": sorted(buckets["single"],  key=lambda x: x["ml_price"] or 9e9)[:top_n]},
            "promo":    {"summary": promo_sum,     "items": sorted(promo_all,          key=lambda x: x["ml_price"] or 9e9)[:top_n]},
            "bundle":   {"summary": {"sample_count": len(buckets["bundle"])}, "items": buckets["bundle"][:top_n]},
            "abnormal": {"summary": abnormal_sum,  "items": sorted(buckets["abnormal"], key=lambda x: x["price"])[:top_n]},
            "unknown":  {"summary": unknown_sum,   "items": buckets["unknown"][:top_n]},
            # 하위 호환 shim
            "summary": single_sum,
            "items":   sorted(buckets["single"], key=lambda x: x["ml_price"] or 9e9)[:top_n],
            # 디버그
            "_debug": {
                "seed_raw":            len(seed_raw),
                "brand_raw":           len(brand_raw),
                "merged_raw":          len(merged),
                "dedup_count":         dedup_count,
                "classified_single":   sn,
                "classified_promo":    len(buckets["promo"]),
                "classified_bundle":   len(buckets["bundle"]),
                "classified_abnormal": an,
                "classified_unknown":  ukn,
                "outlier_moved":       outlier_moved,
            },
        })

    return results


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 12 — 트렌드 / 차트 / Claude
# ══════════════════════════════════════════════════════════════════════════════

def classify_trend_status(change_pct: float | int | None) -> dict:
    if change_pct is None:
        return {"code": "na", "label": "데이터 부족",
                "interp": "비교 기준 주차 데이터 부족", "color": "muted"}
    p = float(change_pct)
    if p >= 50:
        return {"code": "surge", "label": "급등 (시즌 진입)",
                "interp": "시즌 진입 신호 · 수요 급증 구간", "color": "hot"}
    if p >= 20:
        return {"code": "rise",  "label": "상승 (수요 확대)",
                "interp": "수요 확대 구간", "color": "up"}
    if p > -20:
        return {"code": "flat",  "label": "보합",
                "interp": "큰 변화 없음 · 관망 구간", "color": "flat"}
    return {"code": "drop", "label": "하락 (수요 감소)",
            "interp": "수요 감소 신호 · 원인 확인 필요", "color": "down"}


def build_chart_overview(trend_list: list[dict]) -> list[str]:
    if not trend_list:
        return []
    surge = [t for t in trend_list if t.get("status", {}).get("code") == "surge"]
    rise  = [t for t in trend_list if t.get("status", {}).get("code") == "rise"]
    drop  = [t for t in trend_list if t.get("status", {}).get("code") == "drop"]
    lines = []
    if surge:
        top = max(surge, key=lambda t: t.get("change_pct") or 0)
        lines.append(f"▲ {top['keyword']} 급등 (+{top['change_pct']}%) · 시즌 진입 신호")
    if rise:
        names = " · ".join(t["keyword"] for t in rise[:3])
        lines.append(f"상승 키워드: {names}")
    if drop:
        names = " · ".join(t["keyword"] for t in drop[:2])
        lines.append(f"하락 키워드: {names} — 원인 점검 권장")
    if not lines:
        lines.append("전 키워드 보합 구간 — 현재 운영 유지 권장")
    return lines


def build_hero_story(market_summary: dict, trend_list: list[dict]) -> dict:
    kw  = market_summary.get("top_growing_keyword")
    pct = market_summary.get("top_growing_pct")
    if not kw or pct is None:
        return {}
    st = classify_trend_status(pct)
    if st["code"] == "surge":
        action = "상승 키워드 중심으로 예산/재고 우선 점검"
    elif st["code"] == "rise":
        action = "상위 상품 노출과 광고 비중 확대 검토"
    elif st["code"] == "drop":
        action = "수요 하락 원인 점검 및 프로모션 재설계"
    else:
        action = "현재 운영 유지, 주간 추이 관찰"
    return {"trend": f"{kw} {st['label']} ({'+' if pct > 0 else ''}{pct}%)",
            "interp": st["interp"], "action": action,
            "status": st, "keyword": kw, "change_pct": pct}


def mark_partial_weeks(trends: dict) -> dict:
    today = datetime.now().date()
    for r in trends.get("results", []):
        pts = r.get("data", [])
        for pt in pts:
            pt["is_partial"] = False
        if not pts:
            continue
        try:
            raw = str(pts[-1].get("period", ""))[:10]
            last_start = datetime.strptime(raw, "%Y-%m-%d").date()
            if last_start + timedelta(days=6) >= today:
                pts[-1]["is_partial"] = True
                print(f"  [알림] '{r.get('title','')}': 마지막 주차 집계 진행 중 - 변화율 계산에서 제외")
        except Exception as e:
            print(f"  [경고] partial 감지 실패({r.get('title','')}): {e}")
    return trends


def fetch_naver_trends(keywords: list[str]) -> dict:
    url   = "https://openapi.naver.com/v1/datalab/search"
    end   = datetime.now()
    start = end - timedelta(weeks=8)
    body  = {
        "startDate": start.strftime("%Y-%m-%d"),
        "endDate":   end.strftime("%Y-%m-%d"),
        "timeUnit":  "week",
        "keywordGroups": [{"groupName": kw, "keywords": [kw]} for kw in keywords[:5]],
    }
    try:
        resp = requests.post(url, headers=NAVER_HEADERS(), json=body, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        print(f"  [경고] DataLab {resp.status_code}: {resp.text[:120]}")
    except Exception as e:
        print(f"  [경고] DataLab 연결 오류: {e}")
    return {"results": [], "error": True}


def build_chart_data(trends: dict) -> dict:
    results = trends.get("results", [])
    if not results:
        return {"labels": [], "datasets": [], "top3_keywords": [], "has_partial_tail": False}

    period_set: list[str] = []
    seen_p: set = set()
    any_partial = False
    for r in results:
        for pt in r.get("data", []):
            if pt.get("is_partial"):
                any_partial = True
                continue
            p = str(pt.get("period", ""))[:10]
            if p and p not in seen_p:
                seen_p.add(p)
                period_set.append(p)
    period_set.sort()

    colors = [
        ("#c09a5a", "rgba(192,154,90,"),
        ("#1b2c4f", "rgba(27,44,79,"),
        ("#e05c7a", "rgba(224,92,122,"),
        ("#2d9c8a", "rgba(45,156,138,"),
        ("#c0601a", "rgba(192,96,26,"),
    ]

    avg_scores = []
    for r in results:
        cpts = _complete_points(r.get("data", []))
        avg  = sum(p.get("ratio") or 0 for p in cpts) / len(cpts) if cpts else 0
        avg_scores.append((r.get("title", ""), avg))
    top3 = {name for name, _ in sorted(avg_scores, key=lambda x: -x[1])[:3]}

    datasets = []
    for i, r in enumerate(results):
        name = r.get("title", "")
        clr_hex, clr_rgba = colors[i % len(colors)]
        is_top = name in top3
        by_period = {str(pt.get("period", ""))[:10]: pt
                     for pt in r.get("data", []) if not pt.get("is_partial")}
        data = [by_period.get(p, {}).get("ratio") for p in period_set]
        datasets.append({
            "label": name, "data": data,
            "borderColor": clr_hex,
            "backgroundColor": clr_rgba + "0.08)",
            "tension": 0.4, "fill": False,
            "borderWidth": 2.5 if is_top else 1.2,
            "borderDash":  []    if is_top else [5, 4],
            "pointRadius": 3     if is_top else 1,
            "order":       0     if is_top else 1,
            "spanGaps":    True,
        })

    return {"labels": period_set, "datasets": datasets,
            "top3_keywords": list(top3), "has_partial_tail": any_partial}


def compute_market_summary(trends: dict, own_data: list[dict],
                           competitor_data: list[dict]) -> dict:
    results  = trends.get("results", [])
    changes  = []
    top_kw, top_pct = "", 0.0
    for r in results:
        cpts = _complete_points(r.get("data", []))
        if len(cpts) >= 2:
            recent = cpts[-1].get("ratio") or 0
            prev   = cpts[-2].get("ratio") or 0
            if prev > 0:
                pct = round((recent - prev) / prev * 100, 1)
                changes.append(pct)
                if abs(pct) > abs(top_pct):
                    top_pct, top_kw = pct, r.get("title", "")

    avg_change = round(sum(changes) / len(changes), 1) if changes else 0.0
    comp_count = 0
    for c in competitor_data:
        for k in ("single", "promo", "abnormal"):
            comp_count += (c.get(k, {}) or {}).get("summary", {}).get("sample_count", 0)
    if comp_count == 0:
        comp_count = sum(
            (c.get("summary", {}) or {}).get("sample_count", len(c.get("items", [])))
            for c in competitor_data
        )

    period = "최근 8주"
    all_pts = [pt.get("period", "") for r in results for pt in r.get("data", [])]
    if all_pts:
        period = f"{min(all_pts)[:10]} ~ {max(all_pts)[:10]}"

    return {
        "avg_index_change_pct": avg_change,
        "top_growing_keyword":  top_kw,
        "top_growing_pct":      top_pct,
        "competitor_count":     comp_count,
        "own_product_count":    len(own_data),
        "data_period":          period,
        "index_note":           "100 = 해당 기간 내 최고 검색량을 기준으로 정규화한 상대 지수",
        "has_trend_data":       len(results) > 0,
    }


_BANNED_TO_SAFE = [
    (re.compile(r"광고\s*확정"),            "광고 확인 필요"),
    (re.compile(r"광고\s*과열"),            "광고 현황 점검 필요"),
    (re.compile(r"광고\s*부족"),            "광고 현황 점검 필요"),
    (re.compile(r"비공식\s*유통\s*발생"),    "비공식 유통 여부 점검 필요"),
    (re.compile(r"비공식\s*유통[이가]?\s*확정"), "비공식 유통 여부 점검 필요"),
    (re.compile(r"비수기\s*진입\s*확정"),    "비수기 전환 가능성 높음"),
    (re.compile(r"성수기\s*진입\s*확정"),    "성수기 전환 가능성 높음"),
    (re.compile(r"확정(?=[^\w]|$)"),        "가능성"),
]


def _sanitize_str(s: str) -> str:
    if not isinstance(s, str):
        return s
    for pat, repl in _BANNED_TO_SAFE:
        s = pat.sub(repl, s)
    return s


def _sanitize_insights(ins: dict) -> dict:
    def walk(v):
        if isinstance(v, str):   return _sanitize_str(v)
        if isinstance(v, list):  return [walk(x) for x in v]
        if isinstance(v, dict):  return {k: walk(vv) for k, vv in v.items()}
        return v

    ins = walk(ins) if isinstance(ins, dict) else {}
    ins.setdefault("one_line", "")
    ins.setdefault("hot_keywords", [])
    ins["insights"]    = (ins.get("insights") or [])[:3]
    ins["action_items"] = (ins.get("action_items") or [])[:3]
    ins["risks"]        = (ins.get("risks") or [])[:2]
    ins.setdefault("market_outlook", "")
    ins.setdefault("priority_checks", [])
    for it in ins["insights"]:
        it.setdefault("data_change", ""); it.setdefault("interpretation", "")
        it.setdefault("confirm_needed", ""); it.setdefault("action", "")
    for a in ins["action_items"]:
        a.setdefault("priority", "중간"); a.setdefault("action", ""); a.setdefault("deadline", "")
    return ins


def _fallback_insights() -> dict:
    return {
        "one_line": "AI 분석을 가져오지 못했습니다. API 키를 확인하세요.",
        "hot_keywords": [], "insights": [], "action_items": [],
        "risks": [], "market_outlook": "", "priority_checks": [], "error": True,
    }


def _compute_priority_checks(own_data: list[dict], competitor_data: list[dict]) -> list[dict]:
    empty_own = sum(1 for p in own_data if not p.get("has_data"))
    bad_codes = {"no_match", "market_only", "no_source", "verify", "not_visible"}
    bad_comp  = sum(1 for p in own_data
                    if p.get("comp_status", {}).get("code") in bad_codes)
    return [
        {
            "label": "데이터 공백",
            "count": empty_own,
            "detail": f"자사 SKU {empty_own}개 쇼핑 데이터 미수집" if empty_own else "모든 SKU 데이터 수집 완료",
        },
        {
            "label": "노출 점검 필요",
            "count": bad_comp,
            "detail": f"{bad_comp}개 SKU 자사 노출 상태 점검 필요" if bad_comp else "자사 노출 상태 정상",
        },
        {
            "label": "광고 미연동",
            "count": 1,
            "detail": "광고 데이터는 현재 미연동 상태로 광고 기반 판단은 제외",
        },
    ]


def analyze_with_claude(config: dict, trends: dict, own_data: list[dict],
                        competitor_data: list[dict]) -> dict:
    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    system_prompt = (
        "당신은 화장품 유통 마켓 애널리스트입니다.\n\n"
        "【데이터 범위】\n"
        "- 제공: 네이버 DataLab 검색 지수, 네이버 쇼핑 가격\n"
        "- 미제공: 재고·매출·고객 데이터\n\n"
        "【표현 규칙】 필수\n"
        "- 확정 표현 금지: '~이다 / 확정 / 발생 / 진입했다' 금지\n"
        "- 가능성 표현 사용: '~가능성', '~여부 확인 필요', '~시그널'\n"
        "- 재고 관련: '재고 확인 필요' 형태만 허용. 재고 소진/과잉 단정 금지\n"
        "- 비공식 유통: '점검 필요'로만 표현\n\n"
        "【길이 규칙】 초과 금지\n"
        "- one_line <= 50자\n"
        "- insights 각 필드 <= 50자\n"
        "- risks 각 <= 35자 / action_items.action <= 30자 / market_outlook <= 90자\n\n"
        "응답은 반드시 유효한 JSON 한 개만 반환하세요."
    )

    trend_summary = []
    for r in trends.get("results", []):
        cpts = _complete_points(r.get("data", []))
        if len(cpts) >= 2:
            recent = cpts[-1].get("ratio") or 0
            prev   = cpts[-2].get("ratio") or 0
            avg8   = round(sum(p.get("ratio") or 0 for p in cpts) / len(cpts), 1)
            pct    = round((recent - prev) / prev * 100, 1) if prev > 0 else 0
            trend_summary.append({
                "keyword": r.get("title", ""), "recent_index": recent,
                "prev_index": prev, "change_pct": pct, "avg8w_index": avg8,
                "trend": "상승" if pct > 5 else "하락" if pct < -5 else "보합",
            })

    price_summary = []
    for p in own_data:
        if not p.get("has_data"):
            continue
        our = p.get("our_price") or 0
        price_summary.append({
            "product": p.get("name", ""), "series": p.get("series", ""),
            "our_price": our, "market_min": p.get("min_price") or 0,
            "own_query_hits": p.get("own_query_hits", 0),
        })

    comp_summary = [
        {
            "category": c.get("category", ""),
            "single_count": (c.get("single", {}).get("summary") or {}).get("sample_count", 0),
            "ref_ml_median": (c.get("price_ref") or {}).get("median_ml", 0),
        }
        for c in competitor_data
    ]

    user_msg = f"""
다음 데이터를 분석하세요.

## 키워드 검색 지수 (완전 주차 기준)
{json.dumps(trend_summary, ensure_ascii=False, indent=2)}

## 자사 SKU 노출 현황
{json.dumps(price_summary, ensure_ascii=False, indent=2)}

## 경쟁사 카테고리 단품 현황
{json.dumps(comp_summary, ensure_ascii=False, indent=2)}

아래 JSON 한 개만 반환하세요.
{{
  "one_line": "핵심 한 줄 (가능성 표현, <= 50자)",
  "hot_keywords": ["상위 3개 이하"],
  "insights": [
    {{
      "data_change": "수치 변화 (<= 40자)",
      "interpretation": "가능성 해석 (<= 50자)",
      "confirm_needed": "추가 확인 사항 (<= 35자)",
      "action": "실행 액션 (<= 30자)"
    }}
  ],
  "action_items": [
    {{"priority": "높음|중간|낮음", "action": "<= 30자", "deadline": "권장 기한"}}
  ],
  "risks": ["<= 35자", "<= 35자"],
  "market_outlook": "2문장 이내 (<= 90자)"
}}
"""

    try:
        resp = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=3000,
            system=[{"type": "text", "text": system_prompt,
                      "cache_control": {"type": "ephemeral"}}],
            messages=[{"role": "user", "content": user_msg}],
        )
        raw = resp.content[0].text.strip()
        raw = re.sub(r"```json\s*", "", raw)
        raw = re.sub(r"```\s*",     "", raw).strip()
        parsed = None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            s = raw.find("{"); e = raw.rfind("}")
            if s != -1 and e > s:
                try:
                    parsed = json.loads(raw[s:e + 1])
                except json.JSONDecodeError as e2:
                    print(f"  [경고] JSON 파싱 실패: {e2}")
        if parsed is not None:
            return _sanitize_insights(parsed)
    except Exception as e:
        print(f"  [경고] Claude API 오류: {e}")

    return _fallback_insights()


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 9 — VIEW MODEL BUILDER
# ══════════════════════════════════════════════════════════════════════════════

def _build_trend_list(trends: dict, chart_data: dict) -> list[dict]:
    """트렌드 키워드 리스트 + 상태 분류."""
    trend_list = []
    top3_kws   = set(chart_data.get("top3_keywords", []))
    for r in trends.get("results", []):
        pts         = r.get("data", [])
        cpts        = _complete_points(pts)
        partial_flag = bool(pts and pts[-1].get("is_partial"))
        if len(cpts) >= 2:
            recent = cpts[-1].get("ratio") or 0
            prev   = cpts[-2].get("ratio") or 0
            pct    = round((recent - prev) / prev * 100, 1) if prev > 0 else 0
            avg_base = cpts
        elif cpts:
            recent = cpts[-1].get("ratio") or 0
            prev = recent; pct = 0; avg_base = cpts
        else:
            recent = prev = pct = 0; avg_base = pts
        avg8   = round(sum(p.get("ratio") or 0 for p in avg_base) / len(avg_base), 1) if avg_base else 0
        status = classify_trend_status(pct if len(cpts) >= 2 else None)
        trend_list.append({
            "keyword":      r.get("title", ""),
            "recent":       recent,
            "change_pct":   pct,
            "avg8":         avg8,
            "up":           pct > 0,
            "is_top3":      r.get("title", "") in top3_kws,
            "partial_flag": partial_flag,
            "status":       status,
        })
    return trend_list


def build_report_view_model(
    config: dict,
    trends: dict,
    own_data: list[dict],
    competitor_data: list[dict],
    insights: dict,
    market_summary: dict,
    sku_meta: "dict | None" = None,
) -> dict:
    """
    Layer 9: 뷰 모델 빌더.
    템플릿에 필요한 모든 값을 여기서 계산한다.
    템플릿은 출력만 담당한다.
    """
    now = datetime.now()

    # ── 트렌드 차트 ──────────────────────────────────────────────────────────
    chart_data     = build_chart_data(trends)
    trend_list     = _build_trend_list(trends, chart_data)
    chart_overview = build_chart_overview(trend_list)
    hero_story     = build_hero_story(market_summary, trend_list)

    # ── 자사 제품 시리즈 그룹 ─────────────────────────────────────────────────
    series_map: dict[str, list] = {}
    for p in own_data:
        series_map.setdefault(p.get("series", "기타"), []).append(p)
    categories = sorted({p.get("category", "") for p in own_data if p.get("category")})

    # ── 카테고리별 참조가 주입 ────────────────────────────────────────────────
    cat_price_ref: dict[str, dict] = {}
    for c in competitor_data:
        cat = c["category"]
        pr  = c.get("price_ref") or {}
        cat_price_ref[cat] = {
            "avg_ml":    pr.get("avg_ml",    0),
            "median_ml": pr.get("median_ml", 0),
            "p25_ml":    pr.get("p25_ml",    0),
        }

    for p in own_data:
        cat    = p.get("category", "")
        vol_ml = p.get("volume_ml") or 0
        if not vol_ml:
            vol_str = str(p.get("volume") or "")
            vm = re.search(r"(\d+(?:\.\d+)?)", vol_str)
            vol_ml = float(vm.group(1)) if vm else 0

        ref       = cat_price_ref.get(cat, {})
        med_ml    = ref.get("median_ml", 0)
        avg_ml    = ref.get("avg_ml",    0)
        p25_ml    = ref.get("p25_ml",    0)

        # 세 가지 참조가: primary=median, secondary=avg, defensive=p25
        p["cat_avg_ref_price"]    = round(avg_ml * vol_ml)    if (avg_ml > 0 and vol_ml > 0)  else 0
        p["cat_median_ref_price"] = round(med_ml * vol_ml)    if (med_ml > 0 and vol_ml > 0)  else 0
        p["cat_p25_ref_price"]    = round(p25_ml * vol_ml)    if (p25_ml > 0 and vol_ml > 0)  else 0
        p["cat_comp_ml_avg"]      = avg_ml
        p["cat_comp_ml_median"]   = med_ml

        # 템플릿 호환: cat_avg_ref_price = median (더 안정적)
        p["cat_avg_ref_price"]  = p["cat_median_ref_price"]

        # SKU 상태 판단 (median 기준)
        p["status_info"] = _sku_status(p, med_ml)

    # ── canonical 브랜드 리스트 ──────────────────────────────────────────────
    _amap     = _build_alias_map(config.get("competitor_brand_aliases", []))
    _seen_c:  set = set()
    canonical_brands: list[str] = []
    for b in config.get("competitor_brands", []):
        canon = _canonical_brand(b, _amap)
        if canon not in _seen_c:
            _seen_c.add(canon)
            canonical_brands.append(canon)

    # ── 경쟁사 인사이트 ───────────────────────────────────────────────────────
    comp_insights     = [_generate_comp_insight(c, config.get("brand_name", "자사"))
                         for c in competitor_data]
    comp_insights_map = {ins["category"]: ins for ins in comp_insights}

    # ── 경쟁사 시각화 데이터 ─────────────────────────────────────────────────
    comp_viz = []
    for c in competitor_data:
        comp_viz.append({
            "category":   c["category"],
            "brand_data": c.get("brand_data") or [],
            "our_ml":     c.get("our_ml_price") or 0,
            "structure": {
                "single":   (c.get("single",   {}).get("summary") or {}).get("sample_count", 0),
                "promo":    (c.get("promo",     {}).get("summary") or {}).get("sample_count", 0),
                "abnormal": (c.get("abnormal",  {}).get("summary") or {}).get("sample_count", 0),
            },
        })

    # ── SKU 커버리지 ─────────────────────────────────────────────────────────
    sku_coverage = _own_sku_coverage(own_data)

    # 소스 메타 주입
    _src_label_map = {
        "config_fallback": "config fallback",
        "external_json":   "external JSON",
        "external_csv":    "external CSV",
    }
    _meta = sku_meta or {}
    _src  = _meta.get("source", "config_fallback")
    _total_def = _meta.get("total_defined", sku_coverage["total"])
    _active    = _meta.get("active", sku_coverage["total"])
    sku_coverage["source"]        = _src
    sku_coverage["source_label"]  = _src_label_map.get(_src, _src)
    sku_coverage["total_defined"] = _total_def
    sku_coverage["active_count"]  = _active

    _other   = sum(1 for p in own_data if p.get("series") == "기타")
    _total_n = sku_coverage["total"] or 1
    sku_coverage["series_other_count"] = _other
    sku_coverage["series_other_pct"]   = round(_other / _total_n * 100)

    # 커버리지 해석 문구 (소스 포함)
    _src_note = f" ({sku_coverage['source_label']})"
    if sku_coverage["total"] == 0:
        sku_coverage["interp"] = f"SKU 정의 없음{_src_note}"
    elif sku_coverage["unmatched"] == 0:
        sku_coverage["interp"] = (
            f"{sku_coverage['total']}개 중 {sku_coverage['matched']}개 매칭 · 시장 노출 정상{_src_note}"
        )
    else:
        sku_coverage["interp"] = (
            f"{sku_coverage['total']}개 중 {sku_coverage['unmatched']}개 미노출 · "
            f"own_query 키워드 점검 필요{_src_note}"
        )

    # ── 데이터 메타 ───────────────────────────────────────────────────────────
    brand_label = (
        f"시드 키워드 + {len(canonical_brands)}개 브랜드 기반 수집"
        if canonical_brands else "네이버 쇼핑 검색 기반"
    )
    data_meta = {
        "period":          market_summary.get("data_period", "최근 8주"),
        "source":          "네이버 데이터랩 + 네이버 쇼핑",
        "generated_at":    now.strftime("%Y-%m-%d %H:%M"),
        "index_note":      "100 = 해당 기간 내 최고 검색량을 기준으로 정규화한 상대 지수",
        "shopping_note":   f"자사 own_query {len(own_data)}개 SKU 검색",
        "competitor_note": brand_label,
    }

    return {
        # 차트
        "chart_data":          chart_data,
        "chart_data_json":     json.dumps(chart_data, ensure_ascii=False),
        "top3_json":           json.dumps(chart_data["top3_keywords"], ensure_ascii=False),
        "comp_viz_json":       json.dumps(comp_viz, ensure_ascii=False),
        # 트렌드
        "trend_list":          trend_list,
        "chart_overview":      chart_overview,
        "hero_story":          hero_story,
        # 자사
        "own_data":            own_data,
        "series_map":          series_map,
        "categories":          categories,
        "sku_coverage":        sku_coverage,
        # 경쟁사
        "competitor_data":     competitor_data,
        "canonical_brands":    canonical_brands,
        "competitor_brands":   config.get("competitor_brands", []),
        "comp_insights":       comp_insights,
        "comp_insights_map":   comp_insights_map,
        # 인사이트/Claude
        "insights":            insights,
        "market_summary":      market_summary,
        # 메타
        "data_meta":           data_meta,
        "report_title":        config.get("report_title", "Market Monitoring"),
        "company_name":        config.get("company_name", ""),
        "brand_name":          config.get("brand_name", ""),
        "generated_at":        now.strftime("%Y-%m-%d %H:%M"),
        "report_date":         now.strftime("%Y-%m-%d"),
        # 플래그
        "has_trend_data":      bool(trends.get("results")),
        "has_own_data":        any(p.get("has_data") for p in own_data),
        "has_competitor_data": any(c.get("items") for c in competitor_data),
        "has_insights":        not insights.get("error"),
    }


# ══════════════════════════════════════════════════════════════════════════════
# LAYER 10 — RENDER
# ══════════════════════════════════════════════════════════════════════════════

def render_report(
    config: dict,
    trends: dict,
    own_data: list[dict],
    competitor_data: list[dict],
    insights: dict,
    market_summary: dict,
    sku_meta: "dict | None" = None,
) -> str:
    """Layer 10: 순수 렌더링. 계산 없음. build_report_view_model() 결과를 템플릿에 전달."""
    env = Environment(loader=FileSystemLoader("templates"))

    vm = build_report_view_model(
        config, trends, own_data, competitor_data, insights, market_summary,
        sku_meta=sku_meta,
    )

    try:
        template = env.get_template("report.html")
        return template.render(**vm)
    except Exception as e:
        # 폴백 HTML
        dm     = vm.get("data_meta", {})
        parts  = [
            "<html><head><meta charset='utf-8'>"
            "<title>Competitor Market Analysis</title></head><body>",
            f"<h1>{html.escape(config.get('report_title', 'Market Monitoring'))}</h1>",
            f"<p>Source: {html.escape(dm.get('source', ''))}</p>",
            f"<p>Template error: {html.escape(str(e))}</p>",
            "<h2>Competitor Market Analysis</h2>",
        ]
        for comp in competitor_data:
            items = comp.get("items") or []
            if not items:
                continue
            s = comp.get("summary") or {}
            parts.append(f"<h3>{html.escape(str(comp.get('category', '-')))}</h3>")
            parts.append(
                f"<ul><li>단품: {s.get('sample_count',0)}개</li>"
                f"<li>평균 ml가: {s.get('avg_ml_price',0)}원/ml</li></ul>"
            )
            parts.append("<ol>")
            for item in items:
                parts.append(
                    f"<li>{html.escape(str(item.get('title','')))} / "
                    f"{html.escape(str(item.get('mall','')))} / "
                    f"{item.get('price',0):,} KRW</li>"
                )
            parts.append("</ol>")
        parts.append("</body></html>")
        return "".join(parts)


# ══════════════════════════════════════════════════════════════════════════════
# MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    import sys
    if sys.platform == "win32" and hasattr(sys.stdout, "reconfigure"):
        try:
            sys.stdout.reconfigure(errors="replace")
        except Exception:
            pass

    print("=" * 52)
    print("  뷰티 마켓 모니터링 리포트 생성")
    print("=" * 52)

    config    = load_config()
    Path("reports").mkdir(exist_ok=True)

    use_naver  = bool(NAVER_CLIENT_ID and NAVER_CLIENT_SECRET)
    use_claude = bool(ANTHROPIC_API_KEY)
    own_brand  = config.get("our_brand_keyword", "")
    exclude    = config.get("exclude_keywords", [])

    _src_labels = {
        "config_fallback":  "config fallback",
        "external_json":    "external JSON",
        "external_csv":     "external CSV",
        "naver_discovery":  "Naver 자동 발견",
    }

    # ── 트렌드 수집 ───────────────────────────────────────────────────────────
    print(f"\n[1/4] 네이버 DataLab 트렌드 수집...")
    if use_naver:
        trends = fetch_naver_trends(config.get("keywords", []))
        print(f"  -> {len(trends.get('results', []))}개 키워드 완료")
    else:
        print("  -> API 키 없음, 샘플 데이터 사용")
        trends = _sample_trends(config.get("keywords", []))
    trends = mark_partial_weeks(trends)

    # ── Layers 1+2: 자사 SKU 수집 ────────────────────────────────────────────
    if use_naver:
        disc_n = config.get("naver_shopping", {}).get("own_discovery_items", 40)
        print(f"\n[2/4] 자사 SKU 자동 발견 ({own_brand} x 카테고리 Naver 검색)...")
        own_data, sku_meta = collect_own_from_naver(
            brand=own_brand,
            competitor_searches=config.get("competitor_searches", []),
            exclude=exclude,
            series_map=config.get("own_series_map", {}),
            mq_templates=config.get("market_query_templates", {}),
            display=disc_n,
        )
        for d in own_data:
            try:
                print(f"  -> {d['name'][:40]}: {d['our_price']:,}원 [{d['series']} / {d['category']}]")
            except UnicodeEncodeError:
                safe = d['name'][:40].encode('cp949', errors='replace').decode('cp949')
                print(f"  -> {safe}: {d['our_price']:,}원 [{d['series']} / {d['category']}]")
    else:
        print(f"\n[2/4] 자사 상품 로드 (샘플 데이터)...")
        skus, sku_meta = load_own_sku_master(config)
        own_data = [_sample_sku(sku) for sku in skus]
        src_disp  = _src_labels.get(sku_meta.get("source", ""), sku_meta.get("source", ""))
        total_def = sku_meta.get("total_defined", len(skus))
        print(f"  -> {len(skus)}개 active / {total_def}개 정의, {src_disp}")

    # ── Layers 3~8: 경쟁사 시장 파이프라인 ────────────────────────────────────
    print(f"\n[3/4] 경쟁사 시장 수집 (시드+브랜드 파이프라인)...")
    comp_top_n = config.get("display", {}).get("competitor_top_n", 5)
    if use_naver:
        competitor_data = build_competitor_market(
            config.get("competitor_searches", []),
            config,
            own_data,
            top_n=comp_top_n,
        )
    else:
        competitor_data = _sample_competitors(config)
        print("  -> 샘플 경쟁사 데이터 사용")

    # ── Claude 분석 ──────────────────────────────────────────────────────────
    print(f"\n[4/4] Claude AI 분석...")
    market_summary = compute_market_summary(trends, own_data, competitor_data)
    if use_claude:
        insights = analyze_with_claude(config, trends, own_data, competitor_data)
        print("  -> AI 분석 완료")
    else:
        insights = _fallback_insights()
        print("  -> API 키 없음, fallback 사용")

    insights["priority_checks"] = _compute_priority_checks(own_data, competitor_data)

    # ── Layer 10: 리포트 생성 ─────────────────────────────────────────────────
    html_content = render_report(config, trends, own_data, competitor_data, insights, market_summary, sku_meta=sku_meta)
    filename = f"reports/report_{datetime.now().strftime('%Y%m%d_%H%M')}.html"
    with open(filename, "w", encoding="utf-8") as f:
        f.write(html_content)

    print(f"\n[완료] 리포트 저장: {filename}")
    print("=" * 52)
    return filename


# ══════════════════════════════════════════════════════════════════════════════
# 샘플 데이터 (API 키 없을 때 폴백)
# ══════════════════════════════════════════════════════════════════════════════

def _sample_trends(keywords: list[str]) -> dict:
    import random
    results = []
    end = datetime.now()
    for kw in keywords:
        pts   = []
        ratio = random.uniform(40, 80)
        for i in range(8):
            d     = end - timedelta(weeks=8 - i)
            ratio = max(5, min(100, ratio + random.uniform(-10, 12)))
            pts.append({"period": d.strftime("%Y-%m-%d 00:00:00"), "ratio": round(ratio, 2)})
        results.append({"title": kw, "data": pts})
    return {"results": results}


def _sample_sku(sku: dict) -> dict:
    import random
    base  = sku.get("our_price") or sku.get("official_price") or 25000
    items = []
    brand = "OUR_BRAND"
    for i in range(4):
        price  = int(base * random.uniform(0.7, 1.1))
        is_our = i < 2
        items.append({
            "title": f"{brand if is_our else 'COMP'} 테스트상품{i+1}",
            "mall":  ["쿠팡", "네이버", "올리브영", brand][i],
            "price": price, "link": "#", "is_ours": is_our,
        })
    items.sort(key=lambda x: x["price"])
    our_items = [x for x in items if x["is_ours"]]
    prices    = [x["price"] for x in items]
    r = {
        **_empty_sku_result(sku),
        "market_items":   items,
        "our_items":      our_items,
        "own_query_hits": len(our_items),
        "total_hits":     len(items),
        "min_price":      min(prices),
        "max_price":      max(prices),
        "our_min_price":  min(x["price"] for x in our_items) if our_items else 0,
        "has_data":       True,
    }
    r["comp_status"] = _classify_comp(r)
    return r


def _sample_competitors(config: dict) -> list[dict]:
    import random
    results    = []
    top_n      = config.get("display", {}).get("competitor_top_n", 5)
    comp_brands = config.get("competitor_brands", [])
    for s in config.get("competitor_searches", []):
        cat     = s.get("category", "")
        vmap    = config.get("category_volume_ranges", {})
        vr      = vmap.get(cat, {})
        vol_min = float(vr.get("min_ml", 10))
        vol_max = float(vr.get("max_ml", 500))
        s_brands = comp_brands[:top_n] if comp_brands else [f"브랜드{i+1}" for i in range(top_n)]
        single_items = [
            {"title": f"{b} {cat} 샘플", "mall": f"쇼핑몰{i+1}",
             "price": random.randint(10000, 40000),
             "brand": b, "link": "#",
             "vol_ml": (vol_min + vol_max) / 2, "total_ml": (vol_min + vol_max) / 2,
             "ml_price": round(random.uniform(200, 900), 1), "type": "single"}
            for i, b in enumerate(s_brands)
        ]
        prices    = [it["price"]    for it in single_items]
        ml_prices = [it["ml_price"] for it in single_items]
        n_s = len(single_items)
        ml_sorted = sorted(ml_prices)
        mid = n_s // 2
        med_ml = ml_sorted[mid] if n_s % 2 else (ml_sorted[mid-1] + ml_sorted[mid]) / 2 if n_s else 0
        single_sum = {
            "brand_count": len(s_brands), "sample_count": n_s,
            "avg_price":   int(sum(prices) / n_s) if prices else 0,
            "median_price": int(_median(prices)) if prices else 0,
            "avg_ml_price": round(sum(ml_prices) / n_s, 1) if ml_prices else 0,
            "median_ml_price": round(_median(ml_prices), 1) if ml_prices else 0,
            "enough_data": True,
            "reliability": {"tier": "medium", "label": "Medium", "color": "amber", "enough": True},
        }
        price_ref = {
            "avg_ml":    single_sum["avg_ml_price"],
            "median_ml": single_sum["median_ml_price"],
            "p25_ml":    round(ml_sorted[max(0, n_s // 4)], 1) if ml_sorted else 0,
            "avg_ref":   0, "median_ref": 0, "p25_ref": 0,
            "sample_count": n_s, "enough_data": True,
            "reliability": single_sum["reliability"],
        }
        brand_data = [
            {"brand": it["brand"], "avg_ml_price": it["ml_price"], "count": 1, "is_ours": False}
            for it in single_items
        ]
        empty_sum = {
            "brand_count": 0, "sample_count": 0, "avg_price": 0, "median_price": 0,
            "avg_ml_price": 0, "median_ml_price": 0, "enough_data": False,
            "reliability": {"tier": "low", "label": "Low", "color": "red", "enough": False},
        }
        results.append({
            "category":        cat,
            "query":           s.get("query", ""),
            "criteria":        "샘플 데이터 (실제 API 연결 시 자동 업데이트)",
            "our_avg":         0, "our_ml_price": 0, "our_position": "-",
            "vol_range":       {"min": vol_min, "max": vol_max},
            "single_out_cnt":  0,
            "brand_data":      brand_data,
            "coverage":        {"requested": len(s_brands), "found": len(s_brands)},
            "price_ref":       price_ref,
            "single":   {"summary": single_sum, "items": single_items[:top_n]},
            "promo":    {"summary": empty_sum,   "items": []},
            "bundle":   {"summary": empty_sum,   "items": []},
            "abnormal": {"summary": empty_sum,   "items": []},
            "unknown":  {"summary": empty_sum,   "items": []},
            # 하위 호환
            "summary": single_sum,
            "items":   single_items[:top_n],
        })
    return results


if __name__ == "__main__":
    main()
