# =============================================================================
# 파일명 : 판교_2반_천성훈.py
# 작성자 : 천성훈
# 작성일 : 2026-08-03
# 설명   : Day 1 종합 실습 - 공개 API 비동기 수집, Pydantic 검증, CSV/Parquet 저장,
#          그리고 두 저장 형식의 성능(쓰기·읽기 시간, 파일 크기) 비교
# 실행   : .venv/bin/python '판교_2반_천성훈.py'
#
# [주석 규칙]
# 1) 섹션 구분  : "# ----" 구분선 + "숫자. 제목" 형태로 큰 단계를 나눈다.
# 2) 함수 docstring : 첫 줄 한 줄 요약 + (필요 시) 상세 설명 + Args/Returns/Raises.
#                    - Args   : 매개변수가 있을 때만 작성한다.
#                    - Returns: None이 아닌 값을 반환할 때만 작성한다.
#                    - Raises : 의도적으로 예외를 발생시킬 때만 작성한다.
# 3) 클래스 docstring : 역할을 설명하는 한 줄 요약만 작성한다(필드는 Field로 자기 설명).
# 4) 인라인 주석 : 코드가 "무엇을" 하는지가 아니라 "왜" 그렇게 작성했는지를 설명하며,
#                해당 코드 바로 위 줄에 '~다.'체 문장과 마침표로 작성한다.
# =============================================================================

"""Day 1 종합 실습: 공개 API 비동기 수집, 검증, 저장 및 성능 비교.

실습 흐름
1. ``asyncio``와 ``httpx``로 3개 API를 동시에 호출한다.
2. 응답 JSON에서 필요한 필드만 추출한다.
3. Pydantic v2 모델로 자료형과 값의 범위를 검증한다.
4. 검증된 데이터를 CSV와 Parquet으로 저장한다.
5. 두 파일 형식의 읽기/쓰기 시간과 파일 크기를 비교한다.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
from pathlib import Path
from time import perf_counter
from typing import Any

import httpx
import pandas as pd
import pytest
from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

# ---------------------------------------------------------------------------
# 1. 수집 대상과 기본 설정
# ---------------------------------------------------------------------------

# URL을 한곳에서 관리하면 주소가 변경됐을 때 수정하기 쉽다.
API_URLS = {
    "weather": (
        "https://api.open-meteo.com/v1/forecast"
        "?latitude=37.5665&longitude=126.9780"
        "&hourly=temperature_2m,precipitation_probability"
        "&forecast_days=3&timezone=Asia%2FSeoul"
    ),
    "country": "https://countries.dev/alpha/KOR",
    "location": "http://ip-api.com/json/8.8.8.8",
}

# 생성되는 파일은 소스 코드와 섞이지 않도록 output 폴더에 저장한다.
OUTPUT_DIR = Path(__file__).resolve().parent / "output"


def print_stage(number: int, title: str) -> None:
    """현재 진행 단계를 구분선과 번호로 터미널에 출력한다.

    Args:
        number: 전체 단계 중 현재 단계 번호.
        title: 현재 단계를 설명하는 제목.
    """
    print(f"\n{'=' * 64}")
    print(f"[{number}/5] {title}")
    print("=" * 64, flush=True)


# ---------------------------------------------------------------------------
# 2. Pydantic v2 스키마: 타입과 값의 범위를 선언한다.
# ---------------------------------------------------------------------------


class StrictModel(BaseModel):
    """모든 실습 모델이 공통으로 사용하는 엄격한 설정."""

    # extra="forbid": 모델에 선언하지 않은 필드가 들어오면 오류로 처리한다.
    # str_strip_whitespace=True: 문자열 앞뒤의 불필요한 공백을 제거한다.
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)


class WeatherRecord(StrictModel):
    """Open-Meteo의 시간대별 날씨 한 건."""

    time: str = Field(min_length=16, description="Asia/Seoul 기준 관측 시각")
    temperature_2m: float = Field(ge=-100, le=100)
    precipitation_probability: int = Field(ge=0, le=100)

    @field_validator("time")
    @classmethod
    def validate_iso_time(cls, value: str) -> str:
        """시간 문자열이 Open-Meteo의 YYYY-MM-DDTHH:MM 형태인지 확인한다.

        Args:
            value: 검증할 시간 문자열.

        Returns:
            검증을 통과한 시간 문자열 원본.

        Raises:
            ValueError: 날짜/시간 형식으로 해석할 수 없을 때.
        """
        try:
            pd.Timestamp(value)
        except ValueError as error:
            raise ValueError("올바른 날짜/시간 문자열이 아닙니다.") from error
        return value


class CountryRecord(StrictModel):
    """Countries.dev 응답에서 실습에 필요한 대한민국 정보."""

    alpha2_code: str = Field(pattern=r"^[A-Z]{2}$")
    alpha3_code: str = Field(pattern=r"^[A-Z]{3}$")
    name: str = Field(min_length=1)
    native_name: str = Field(min_length=1)
    capital: str = Field(min_length=1)
    region: str = Field(min_length=1)
    population: int = Field(gt=0)
    area: float = Field(gt=0)
    currency_code: str = Field(pattern=r"^[A-Z]{3}$")


class LocationRecord(StrictModel):
    """ip-api 응답에서 추출한 IP 기반 위치 정보."""

    ip: str = Field(min_length=7)
    country: str = Field(min_length=1)
    region: str = Field(min_length=1)
    city: str = Field(min_length=1)
    latitude: float = Field(ge=-90, le=90)
    longitude: float = Field(ge=-180, le=180)
    timezone: str = Field(min_length=1)


class FetchResult(StrictModel):
    """API 한 곳의 수집 성공/실패를 동일한 구조로 표현한다."""

    name: str
    ok: bool
    data: dict[str, Any] | None = None
    error: str | None = None


# ---------------------------------------------------------------------------
# 3. 비동기 수집: 세 HTTP 요청을 동시에 실행한다.
# ---------------------------------------------------------------------------


async def fetch_json(client: httpx.AsyncClient, name: str, url: str) -> FetchResult:
    """한 API를 호출하여 JSON을 반환한다.

    네트워크 오류, 타임아웃, 4xx/5xx 응답, 잘못된 JSON을 API별로 처리한다.
    따라서 API 하나가 실패해도 다른 API의 수집 결과는 확인할 수 있다.

    Args:
        client: 요청에 사용할 공용 httpx 비동기 클라이언트.
        name: 결과를 구분하기 위한 API 이름(weather/country/location).
        url: 호출할 API의 전체 URL.

    Returns:
        성공/실패 여부와 데이터(또는 오류 메시지)를 담은 FetchResult.
    """
    try:
        response = await client.get(url)
        response.raise_for_status()  # 4xx 또는 5xx이면 HTTPStatusError 발생
        data = response.json()
        if not isinstance(data, dict):
            raise ValueError("JSON 최상위 값이 객체가 아닙니다.")
        return FetchResult(name=name, ok=True, data=data)
    except (httpx.HTTPError, json.JSONDecodeError, ValueError) as error:
        return FetchResult(name=name, ok=False, error=str(error))


async def collect_all() -> dict[str, FetchResult]:
    """``asyncio.gather``로 세 API를 동시에 수집한다.

    Returns:
        API 이름을 key로, 각 API의 FetchResult를 value로 갖는 딕셔너리.
    """
    # 전체 연결/응답을 무한정 기다리지 않도록 15초 제한을 둔다.
    timeout = httpx.Timeout(15.0)
    headers = {"User-Agent": "day1-api-practice/1.0"}

    async with httpx.AsyncClient(
        timeout=timeout, follow_redirects=True, headers=headers
    ) as client:
        # 코루틴 3개를 먼저 만든 뒤 gather에 전달한다.
        tasks = [fetch_json(client, name, url) for name, url in API_URLS.items()]
        results = await asyncio.gather(*tasks)

    return {result.name: result for result in results}


# ---------------------------------------------------------------------------
# 4. JSON 변환 및 Pydantic 검증
# ---------------------------------------------------------------------------


def require_data(result: FetchResult) -> dict[str, Any]:
    """수집 성공 데이터만 반환하고, 실패 결과에는 명확한 오류를 발생시킨다.

    Args:
        result: 검사할 단일 API의 수집 결과.

    Returns:
        수집에 성공한 JSON 데이터(dict).

    Raises:
        RuntimeError: 수집이 실패했거나 데이터가 없을 때.
    """
    if not result.ok or result.data is None:
        raise RuntimeError(f"{result.name} API 수집 실패: {result.error}")
    return result.data


def validate_weather(data: dict[str, Any]) -> list[WeatherRecord]:
    """배열 형태의 시간·기온·강수확률을 시간별 레코드로 변환·검증한다.

    Args:
        data: weather API 응답 JSON.

    Returns:
        검증을 통과한 WeatherRecord 리스트.

    Raises:
        ValueError: hourly 객체가 없거나 배열 길이가 서로 다를 때.
    """
    hourly = data.get("hourly")
    if not isinstance(hourly, dict):
        raise ValueError("날씨 응답에 hourly 객체가 없습니다.")

    times = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    probabilities = hourly.get("precipitation_probability", [])

    # zip은 가장 짧은 배열 길이에 맞추므로, 먼저 길이가 같은지 검사해야
    # 누락된 데이터를 조용히 버리는 문제를 방지할 수 있다.
    lengths = {len(times), len(temperatures), len(probabilities)}
    if len(lengths) != 1 or not times:
        raise ValueError("날씨 데이터 배열이 비어 있거나 길이가 서로 다릅니다.")

    return [
        WeatherRecord(
            time=time,
            temperature_2m=temperature,
            precipitation_probability=probability,
        )
        for time, temperature, probability in zip(
            times, temperatures, probabilities, strict=True
        )
    ]


def validate_country(data: dict[str, Any]) -> CountryRecord:
    """국가 JSON에서 필요한 필드만 추출하여 검증한다.

    Args:
        data: country API 응답 JSON.

    Returns:
        검증을 통과한 CountryRecord.

    Raises:
        ValueError: 통화 정보가 없을 때.
    """
    currencies = data.get("currencies") or []
    if not currencies or not isinstance(currencies[0], dict):
        raise ValueError("국가 응답에 통화 정보가 없습니다.")

    return CountryRecord(
        alpha2_code=data.get("alpha2Code"),
        alpha3_code=data.get("alpha3Code"),
        name=data.get("name"),
        native_name=data.get("nativeName"),
        capital=data.get("capital"),
        region=data.get("region"),
        population=data.get("population"),
        area=data.get("area"),
        currency_code=currencies[0].get("code"),
    )


def validate_location(data: dict[str, Any]) -> LocationRecord:
    """IP 위치 JSON에서 필요한 필드를 추출하고 좌표 범위를 검증한다.

    Args:
        data: location API 응답 JSON.

    Returns:
        검증을 통과한 LocationRecord.

    Raises:
        ValueError: ip-api가 처리 실패(status != "success")를 반환했을 때.
    """
    # ip-api는 HTTP 200에서도 status="fail"을 반환할 수 있으므로 확인한다.
    if data.get("status") != "success":
        raise ValueError(f"ip-api 처리 실패: {data.get('message', '알 수 없는 오류')}")

    return LocationRecord(
        ip=data.get("query"),
        country=data.get("country"),
        region=data.get("regionName"),
        city=data.get("city"),
        latitude=data.get("lat"),
        longitude=data.get("lon"),
        timezone=data.get("timezone"),
    )


def validate_all(results: dict[str, FetchResult]) -> dict[str, pd.DataFrame]:
    """세 응답을 검증한 뒤 저장에 적합한 DataFrame으로 변환한다.

    Args:
        results: collect_all이 반환한 API별 수집 결과.

    Returns:
        API 이름을 key로, 검증된 데이터의 DataFrame을 value로 갖는 딕셔너리.
    """
    weather = validate_weather(require_data(results["weather"]))
    country = validate_country(require_data(results["country"]))
    location = validate_location(require_data(results["location"]))

    # mode="json"은 Pydantic 값을 JSON 호환 기본 타입으로 바꿔준다.
    return {
        "weather": pd.DataFrame([item.model_dump(mode="json") for item in weather]),
        "country": pd.DataFrame([country.model_dump(mode="json")]),
        "location": pd.DataFrame([location.model_dump(mode="json")]),
    }


# ---------------------------------------------------------------------------
# 5. CSV/Parquet 저장과 성능 측정
# ---------------------------------------------------------------------------

# 이 실험의 핵심은 "같은 데이터를 저장 형식만 다르게 했을 때"를 비교하는 것이다.
# API에서 검증한 하나의 DataFrame을 CSV와 Parquet으로 각각 저장하고,
# 저장한 파일을 다시 읽어 쓰기 시간·읽기 시간·파일 크기를 측정한다.
#
# CSV(Comma-Separated Values)
# - 쉼표로 값을 구분하는 텍스트 파일이다.
# - 메모장이나 Excel로 쉽게 열 수 있지만, 자료형 정보를 별도로 보존하지 않는다.
#
# Parquet
# - 열(column) 단위로 데이터를 저장하는 이진(binary) 형식이다.
# - 자료형과 스키마를 보존하고 압축과 특정 열 조회에 유리하다.
# - 사람이 메모장으로 바로 읽기는 어렵고, 이 실습에서는 pyarrow로 처리한다.
#
# 주의: 날씨 72행, 국가 1행, IP 1행은 매우 작은 데이터이다.
# Parquet은 초기화와 메타데이터 저장 비용이 있어 이번 결과에서는 CSV보다
# 느리거나 파일이 더 클 수 있다. 대용량 데이터에서 Parquet의 장점이 더 잘 나타난다.


def measure_file_format(
    frame: pd.DataFrame, name: str, file_format: str
) -> dict[str, Any]:
    """동일한 DataFrame을 지정 형식으로 저장·재로드하고 성능을 측정한다.

    Args:
        frame: 저장할 DataFrame.
        name: 데이터셋 이름(파일명 접두사로 사용).
        file_format: 저장 형식("csv" 또는 "parquet").

    Returns:
        데이터셋 이름, 형식, 행 수, 쓰기/읽기 시간(ms), 파일 크기(byte)를
        담은 딕셔너리.

    Raises:
        ValueError: file_format이 "csv"/"parquet"가 아닐 때.
    """
    # 예: name="weather", file_format="csv"면 output/weather.csv가 된다.
    path = OUTPUT_DIR / f"{name}.{file_format}"

    # 1) 쓰기 성능 측정
    # perf_counter()는 시계 시간이 아니라 짧은 작업의 "경과 시간"을
    # 정밀하게 측정하는 타이머다. 저장 직전과 직후 값의 차이를 구한다.
    write_start = perf_counter()
    if file_format == "csv":
        # index=False: DataFrame의 행 번호는 실제 API 데이터가 아니므로 저장하지 않는다.
        # utf-8-sig: Excel에서 한글이 깨질 가능성을 줄여준다.
        frame.to_csv(path, index=False, encoding="utf-8-sig")
    elif file_format == "parquet":
        # pyarrow는 Pandas가 Parquet 파일을 읽고 쓰도록 해주는 엔진이다.
        frame.to_parquet(path, index=False, engine="pyarrow")
    else:
        # 함수에 csv/parquet 외의 값이 넘어오면 잘못된 사용으로 처리한다.
        raise ValueError(f"지원하지 않는 형식입니다: {file_format}")
    write_seconds = perf_counter() - write_start

    # 2) 읽기 성능 측정
    # 방금 저장한 파일을 Pandas로 다시 불러와 loaded DataFrame을 만든다.
    # 저장만 측정하는 것이 아니라 실제 재사용 속도까지 비교하기 위함이다.
    read_start = perf_counter()
    if file_format == "csv":
        loaded = pd.read_csv(path)
    else:
        loaded = pd.read_parquet(path, engine="pyarrow")
    read_seconds = perf_counter() - read_start

    # 3) 비교표에 넣을 결과 생성
    # 초(second)는 값이 너무 작게 보일 수 있어 1,000을 곱해 밀리초(ms)로 표시한다.
    # stat().st_size는 디스크에 생성된 실제 파일 크기를 byte 단위로 반환한다.
    return {
        "dataset": name,
        "format": file_format,
        "rows": len(loaded),
        "write_ms": round(write_seconds * 1_000, 3),
        "read_ms": round(read_seconds * 1_000, 3),
        "size_bytes": path.stat().st_size,
    }


def save_and_compare(frames: dict[str, pd.DataFrame]) -> pd.DataFrame:
    """모든 검증 데이터를 두 형식으로 저장하고 측정 결과를 반환한다.

    Args:
        frames: validate_all이 반환한 데이터셋별 DataFrame.

    Returns:
        데이터셋 x 형식 조합별 성능 측정 결과를 담은 DataFrame.
    """
    # output 폴더가 없으면 생성하고, 있으면 그대로 사용한다.
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    measurements: list[dict[str, Any]] = []

    # weather, country, location DataFrame을 하나씩 꺼낸다.
    for name, frame in frames.items():
        # 중요: 같은 frame을 CSV와 Parquet으로 각각 저장하므로
        # 데이터 내용은 같고 파일 형식만 다른 비교 실험이 된다.
        for file_format in ("csv", "parquet"):
            measurements.append(measure_file_format(frame, name, file_format))

    # 측정 결과 목록을 표(DataFrame)로 변환하여 터미널 출력과 저장에 사용한다.
    comparison = pd.DataFrame(measurements)
    # 보고서에 활용할 수 있도록 비교 결과 자체도 CSV로 저장한다.
    comparison.to_csv(OUTPUT_DIR / "performance_comparison.csv", index=False)
    return comparison


def print_summary(frames: dict[str, pd.DataFrame], comparison: pd.DataFrame) -> None:
    """실행 결과와 성능 비교표를 터미널에 출력한다.

    Args:
        frames: 검증된 데이터셋별 DataFrame.
        comparison: save_and_compare가 반환한 성능 비교 결과.
    """
    print("\n=== 스키마 검증 결과 ===")
    print(f"날씨 데이터: {len(frames['weather'])}건 검증 통과")
    print(f"국가 데이터: {len(frames['country'])}건 검증 통과")
    print(f"IP 위치 데이터: {len(frames['location'])}건 검증 통과")
    print("\n=== CSV / Parquet 성능 비교 ===")
    print(comparison.to_string(index=False))
    print(f"\n저장 위치: {OUTPUT_DIR}")


def run_check(command: list[str], label: str) -> bool:
    """품질 검사 명령을 실행하고 결과를 현재 터미널에 이어서 표시한다.

    Args:
        command: 실행할 명령과 인자 목록.
        label: 결과 메시지에 표시할 검사 이름.

    Returns:
        명령이 종료 코드 0으로 끝났으면 True, 아니면 False.
    """
    print(f"\n$ {' '.join(command)}", flush=True)
    completed = subprocess.run(command, check=False)
    if completed.returncode == 0:
        print(f"✓ {label} 통과")
        return True
    print(f"✗ {label} 실패 (종료 코드: {completed.returncode})")
    return False


def run_quality_checks() -> None:
    """pytest와 ruff를 순서대로 실행하고 Git 커밋 준비 상태를 안내한다."""
    pytest_ok = run_check([sys.executable, "-m", "pytest", "-q"], "pytest")
    ruff_ok = run_check([sys.executable, "-m", "ruff", "check", "."], "ruff")

    if pytest_ok and ruff_ok:
        print("\n✓ 모든 검사가 통과했습니다.")
        print("  Git 저장소라면 변경 내용을 확인한 뒤 커밋하세요:")
        print("  git status && git add .")
        print("  git commit -m 'Complete async API pipeline'")
    else:
        print("\n검사 실패 항목을 수정한 뒤 다시 실행해 주세요.")


# ---------------------------------------------------------------------------
# 6. pytest 스키마 검증 테스트
# ---------------------------------------------------------------------------

# 실제 프로젝트에서는 tests/ 폴더로 분리하는 것이 일반적이지만,
# 이번 실습에서는 제출 코드를 한 파일로 관리하기 위해 함께 작성한다.
# pytest는 test_... 형태의 함수를 찾아 자동으로 실행한다.


def test_정상_날씨_데이터_검증_통과() -> None:
    """정상 범위의 날씨 데이터는 모델 생성에 성공해야 한다."""
    record = WeatherRecord(
        time="2026-08-03T12:00",
        temperature_2m=31.5,
        precipitation_probability=40,
    )
    assert record.temperature_2m == 31.5
    assert record.precipitation_probability == 40


@pytest.mark.parametrize(
    "probability",
    [-1, 101],
)
def test_잘못된_강수확률_검증_실패(probability: int) -> None:
    """강수확률은 반드시 0~100 범위여야 한다.

    Args:
        probability: 범위를 벗어난 강수확률 테스트 값(-1 또는 101).
    """
    with pytest.raises(ValidationError):
        WeatherRecord(
            time="2026-08-03T12:00",
            temperature_2m=31.5,
            precipitation_probability=probability,
        )


def test_날씨_배열_길이_불일치_검증_실패() -> None:
    """날씨 응답의 시간·기온·강수확률 배열 길이는 같아야 한다."""
    invalid_json = {
        "hourly": {
            "time": ["2026-08-03T12:00", "2026-08-03T13:00"],
            "temperature_2m": [31.5],
            "precipitation_probability": [40, 50],
        }
    }
    with pytest.raises(ValueError, match="길이"):
        validate_weather(invalid_json)


def test_잘못된_국가코드_검증_실패() -> None:
    """ISO alpha-2 국가 코드는 대문자 영문 두 글자여야 한다."""
    with pytest.raises(ValidationError):
        CountryRecord(
            alpha2_code="KOREA",
            alpha3_code="KOR",
            name="Korea (Republic of)",
            native_name="대한민국",
            capital="Seoul",
            region="Asia",
            population=51_780_579,
            area=100_210,
            currency_code="KRW",
        )


def test_위도_범위_초과_검증_실패() -> None:
    """위도는 -90~90 범위를 벗어날 수 없다."""
    with pytest.raises(ValidationError):
        LocationRecord(
            ip="8.8.8.8",
            country="United States",
            region="Virginia",
            city="Ashburn",
            latitude=91,
            longitude=-77.5,
            timezone="America/New_York",
        )


# ---------------------------------------------------------------------------
# 7. 전체 파이프라인 실행
# ---------------------------------------------------------------------------


async def main() -> None:
    """수집 → 검증 → 저장 → 성능 비교 순서로 실습을 실행한다.

    각 단계를 print_stage로 구분해 출력하며, 중간에 발생하는
    RuntimeError/ValueError/ValidationError는 이 함수에서 최종적으로 처리해
    학습용 실행이 긴 traceback 없이 원인 메시지로 종료되게 한다.
    """
    try:
        print_stage(1, "환경 준비")
        print(f"✓ Python 실행 파일: {sys.executable}")
        print(f"✓ requirements.txt: {Path('requirements.txt').resolve()}")

        print_stage(2, "비동기 API 수집")
        print("3개 API를 asyncio.gather()로 동시에 호출합니다...", flush=True)
        results = await collect_all()
        for name, result in results.items():
            status = "성공" if result.ok else f"실패 - {result.error}"
            print(f"{'✓' if result.ok else '✗'} {name}: {status}")

        print_stage(3, "Pydantic v2 스키마 검증")
        frames = validate_all(results)
        for name, frame in frames.items():
            print(f"✓ {name}: {len(frame)}건 검증 통과")

        print_stage(4, "CSV / Parquet 저장 및 성능 비교")
        comparison = save_and_compare(frames)
        print_summary(frames, comparison)

        print_stage(5, "테스트 및 코드 스타일 검사")
        run_quality_checks()
    except (RuntimeError, ValueError, ValidationError) as error:
        # 학습용 실행에서는 긴 traceback 대신 원인을 이해하기 쉬운 문장으로 표시한다.
        print(f"\n[실행 실패] {error}")


if __name__ == "__main__":
    # 일반 파이썬 파일에서 비동기 main 함수를 시작하는 표준 방식이다.
    asyncio.run(main())
