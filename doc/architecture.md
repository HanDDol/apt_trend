# Apt Trend 기본 설계서 

chat gpt를 통해서 작성한 Prefect + Streamlit 통합 운영 아키텍처 설계서

## 1. 개요

이 문서는 Prefect를 기반으로 한 데이터 수집 자동화 시스템과 Streamlit 사용자 인터페이스를 통합하여 운영하는 구조를 정의한다.  
총 3개의 Prefect Flow를 기반으로 운영되며, Flow는 각각 다음과 같은 목적을 갖는다:

- `monthly_ingest_flow`: 매월 최신 데이터를 자동 수집
- `on_demand_ingest_flow`: 사용자가 요청한 특정 월 데이터를 즉시 수집
- `recovery_flow`: 누락된 월 데이터를 자동으로 감지하고 복구

Streamlit은 사용자 인터페이스로서 데이터 조회, on-demand 수집 요청, 복구 실행을 지원한다.

## 2. 전체 구성도
```
[Streamlit App]
├─ 사용자 월 선택 → DB 조회
├─ 데이터 없음 → on_demand_ingest_flow 실행
├─ 관리 메뉴 → recovery_flow 수동 실행
│
▼
[Prefect Cloud]
├─ Flow Deployment 등록 정보 관리
└─ Flow Run 생성 요청
│
▼
[Prefect Worker (Private Network)]
└─ Flow 코드 실행 (API 수집, DB 저장)
```

## 3. Prefect Flow 정의

### 3.1 `monthly_ingest_flow`

- 목적: 매월 1일 자동으로 최신 월 데이터를 수집
- 배포 명령어:
    ```bash
    prefect deployment build flows/monthly_ingest_flow.py:monthly_ingest_flow \
    -n "Monthly Ingest" -p "api-pool" --cron "0 0 1 * *"
    ```
### 3.2 on_demand_ingest_flow
- 목적: 사용자가 요청한 특정 월 데이터를 수집
- Flow Signature: on_demand_ingest_flow(month: str)
- 배포 명령어:
    ```bash
    prefect deployment build flows/on_demand_ingest_flow.py:on_demand_ingest_flow \
      -n "On Demand Ingest" -p "api-pool"
    ```


### 3.3 recovery_flow
- 목적: 누락된 월을 자동으로 감지하여 복구 실행
- Flow Signature: recovery_flow()
- 배포 명령어:
    ```bash
    prefect deployment build flows/recovery_flow.py:recovery_flow \
  -n "Recovery Flow" -p "api-pool"
    ```

## 4. Streamlit 구성

### 4.1 사용자 기능
- 조회할 월 선택
- DB에서 데이터 존재 여부 확인
- 데이터가 없으면 on_demand_ingest_flow 실행
- 실행 완료 후 자동 재조회

### 4.2 관리자 기능
- recovery_flow 수동 실행 버튼
- Prefect Flow Run 상태 모니터링

### 4.3 Prefect API 호출 예시

```python
def trigger_prefect_flow(deployment_id: str, parameters: dict = None):
    headers = {"Authorization": f"Bearer {PREFECT_API_KEY}", "Content-Type": "application/json"}
    payload = {"parameters": parameters or {}}
    url = f"https://api.prefect.cloud/api/accounts/{ACCOUNT_ID}/workspaces/{WORKSPACE_ID}/deployments/{deployment_id}/create_flow_run"
    return requests.post(url, headers=headers, json=payload)
```


## 5. 데이터 저장 구조 (PostgreSQL)

```sql
CREATE TABLE monthly_data (
    month TEXT PRIMARY KEY,
    data JSONB,
    collected_at TIMESTAMP DEFAULT NOW()
);
```

- Flow가 실행되어 데이터를 성공적으로 수집하면 이 테이블에 저장
- Streamlit은 이 테이블에서 쿼리하여 데이터 조회

## 6. Prefect Worker 운영

실행 환경
- Prefect Worker는 Docker 또는 Docker Compose로 운영
- Private Network 내에서 실행
- 환경 변수는 .env로 분리 관리

```yaml
# docker-compose.yml
services:
  prefect-worker:
    image: prefecthq/prefect:2-latest
    env_file:
      - .env
    command: prefect worker start -p api-pool

```
## 7. 보안 및 네트워크
- Worker는 Private Subnet에 위치
- 외부와는 Prefect Cloud(443)만 Outbound 허용
- 내부 DB 또는 API 접근은 Worker 내부 Task에서 수행
- API Key와 URL은 .env 또는 Secret Manager를 통해 안전하게 관리

## 8. 향후 확장 방향
- Prefect Flow Run 상태 Streamlit 실시간 표시
- Slack/Email로 수집 성공/실패 알림
- Flow Run 이력 저장용 로그 테이블 운영
- GitHub Actions를 통한 CI 기반 Flow 배포 자동화
