from datetime import datetime

import httpx
import streamlit as st
from sqlalchemy import create_engine, text, Column, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

'''
@todo 1. Dash보드에서 23년 1월부터 25년 현재까지의 월의 목록과 지역 코드에 대한 목록을 가져온다. 
    이 부분은 모두 수동으로 구성한다. 
@todo 2. 
'''


# Database configuration
DATABASE_URL = "postgresql://postgres:%40%40N0wsPost@172.16.30.222:5432/apt_db"

# Create SQLAlchemy engine and base
engine = create_engine(DATABASE_URL)
Base = declarative_base()


def create_tables():
    """Create all tables defined by Base if they don't exist."""
    Base.metadata.create_all(engine)


# Ensure tables exist
create_tables()


class RegionMonthlyData(Base):
    """
    (지역, 연월)에 대해서 데이터 획득을 했는지 여부

    :ivar region_code: The unique code identifying the region.
    :type region_code: str
    :ivar month: The YYYYMM string representing the specific month.
    :type month: str
    :ivar updated_at: The timestamp indicating when the data was last updated.
    :type updated_at: datetime.datetime
    """
    __tablename__ = 'region_monthly_data'
    region_code = Column(String, primary_key=True)
    month = Column(String, primary_key=True)  # YYYYMM
    # summary_data = Column(JSON)  # 예: total, meta info 등
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

def trigger_on_demand_ingest(month: str):
    PREFECT_API_KEY = "your-prefect-api-key"
    DEPLOYMENT_ID = "your-on-demand-deployment-id"
    API_URL = "https://api.prefect.cloud/api/accounts/{account_id}/workspaces/{workspace_id}/deployments/{deployment_id}/create_flow_run"

    headers = {"Authorization": f"Bearer {PREFECT_API_KEY}", "Content-Type": "application/json"}

    payload = {
        "parameters": {
            "month": month
        }
    }

    response = httpx.post(API_URL, headers=headers, json=payload)
    if response.status_code == 201:
        return response.json()["id"]
    else:
        raise RuntimeError(f"Flow 실행 실패: {response.text}")


def db_has_data(region: str, month: str) -> bool:
    """Check if data exists in region_monthly_data table for given region and month.

    Args:
        region: Region code
        month: Month in YYYYMM format

    Returns:
        bool: True if data exists, False otherwise
    """
    create_tables()  # Ensure tables exist before any database operation
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        exists = session.query(RegionMonthlyData).filter(
            RegionMonthlyData.region_code == region,
            RegionMonthlyData.month == month
        ).first() is not None
        return exists
    finally:
        session.close()


def fetch_data_from_db(month: str):
    pass
    # with engine.connect() as connection:
    #     result = connection.execute(
    #         text("SELECT * FROM monthly_data WHERE month = :month"),
    #         {"month": month}
    #     )
    #     return result.fetchall()

def get_region_list():
    create_tables()  # Ensure tables exist before any database operation
    return ["서울특별시", "경기도", "인천광역시"]

def get_recent_months():
    return ["202505", "202504", "2020503", "202502", "202501"]


region = st.selectbox("지역", get_region_list())
month = st.selectbox("월", get_recent_months())

if st.button("데이터 조회"):
    if not db_has_data(region, month):
        st.warning("데이터가 없어 Prefect On-Demand 수집을 요청합니다.")
        flow_run_id = trigger_on_demand_ingest(month)
        st.success(f"Flow 실행 요청됨! Flow Run ID: {flow_run_id}")
    else:
        st.success("DB에서 바로 조회합니다.")
        data = fetch_data_from_db(month)
        # st.dataframe(data)
