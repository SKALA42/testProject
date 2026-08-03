# Day 1 종합 실습: 비동기 API 데이터 파이프라인

Python으로 공개 API 3개를 동시에 호출하고, 수집한 JSON을 검증한 뒤 CSV와
Parquet으로 저장하여 읽기·쓰기 성능을 비교하는 실습 프로젝트입니다.

## 실습 내용

- `asyncio`와 `httpx`를 이용한 비동기 API 수집
- `asyncio.gather()`를 이용한 요청 3개 동시 실행
- Pydantic v2 모델을 이용한 타입 및 값의 범위 검증
- Pandas를 이용한 표 데이터 변환
- 동일한 데이터를 CSV와 Parquet으로 각각 저장
- 두 파일 형식의 읽기 시간, 쓰기 시간, 파일 크기 비교
- `pytest`를 이용한 스키마 검증 테스트
- `ruff`를 이용한 코드 스타일 검사

## 수집 API

| API | 수집 내용 |
|---|---|
| Open-Meteo | 서울의 3일 시간대별 기온과 강수확률 |
| Countries.dev | 대한민국 국가 정보 |
| ip-api | `8.8.8.8` IP 기반 지역 정보 |

## 프로젝트 구성

```text
.
├── 판교_2반_천성훈.py   # 수집·검증·저장·테스트를 포함한 메인 코드
├── requirements.txt     # Python 패키지 목록
├── pyproject.toml       # pytest 및 ruff 설정
├── .gitignore
└── README.md
```

프로그램을 실행하면 `output/` 폴더가 자동으로 생성됩니다. 가상환경, 캐시,
실행 결과 파일은 Git에 포함되지 않습니다.

## 환경 준비

프로젝트 폴더에서 가상환경을 생성하고 필요한 패키지를 설치합니다.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
```

터미널의 Python 별칭 때문에 가상환경이 제대로 적용되지 않는 경우에는 아래처럼
가상환경의 Python 경로를 직접 사용합니다.

```bash
.venv/bin/python -m pip install -r requirements.txt
```

## 프로그램 실행

```bash
.venv/bin/python '판교_2반_천성훈.py'
```

실행 순서는 다음과 같습니다.

1. 세 API를 비동기로 동시에 호출합니다.
2. 응답 JSON에서 필요한 필드를 추출합니다.
3. Pydantic 모델로 자료형과 값의 범위를 검증합니다.
4. 검증된 데이터를 Pandas `DataFrame`으로 변환합니다.
5. 같은 데이터를 CSV와 Parquet으로 각각 저장합니다.
6. 저장된 파일을 다시 읽어 읽기·쓰기 시간과 파일 크기를 비교합니다.

## 스키마 검증

주요 검증 항목은 다음과 같습니다.

- 강수확률: `0~100` 범위
- 기온: `-100~100°C` 범위
- 시간·기온·강수확률 배열의 길이 일치 여부
- ISO 국가 코드 형식
- 인구와 면적의 양수 여부
- 위도: `-90~90` 범위
- 경도: `-180~180` 범위
- API 실패 상태와 필수 필드 누락 여부

## CSV와 Parquet 비교

CSV는 사람이 직접 읽기 쉬운 텍스트 형식입니다. Parquet은 자료형과 스키마를
보존하는 열 기반 바이너리 형식으로, 일반적으로 대용량 데이터의 압축과 분석에
유리합니다.

이번 실습은 같은 `DataFrame`을 두 형식으로 저장하고 다시 불러와 다음 항목을
측정합니다.

- `write_ms`: 파일 저장 시간(밀리초)
- `read_ms`: 파일 읽기 시간(밀리초)
- `size_bytes`: 생성된 파일 크기(바이트)

실습 데이터는 1~72행으로 매우 작기 때문에 Parquet의 초기화 및 메타데이터 비용이
더 크게 나타날 수 있습니다. 따라서 이 실습에서는 CSV가 더 빠르거나 작게 측정될
수 있으며, Parquet의 장점은 대용량 데이터에서 더 잘 나타납니다.

## 생성 결과

```text
output/
├── weather.csv
├── weather.parquet
├── country.csv
├── country.parquet
├── location.csv
├── location.parquet
└── performance_comparison.csv
```

## 테스트 및 코드 스타일 검사

Pydantic 스키마 테스트 실행:

```bash
.venv/bin/python -m pytest
```

코드 스타일 검사:

```bash
.venv/bin/python -m ruff check .
.venv/bin/python -m ruff format --check .
```

현재 검증 결과는 다음과 같습니다.

```text
pytest: 6 passed
ruff: All checks passed
```
