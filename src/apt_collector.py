from prefect import flow, task
from prefect.cache_policies import INPUTS

from datetime import timedelta, datetime
from typing import List, Dict, Optional
import httpx

from bs4 import BeautifulSoup
from sqlalchemy import create_engine, Column, Integer, String, Float, Date
from sqlalchemy.orm import sessionmaker, declarative_base

'''
    목적
        - 구/동별 아파트 가격에 대한 대쉬보드를 만들어보자

    단계
        1. 일단 가격 데이터를 모두 가져와서 DB에 쌓아 보는 것부터 시작
            a. Flow 1 : 법정동 데이터를 가져오기
            b. Flow 2 : 법정동 코드 별로 아파트 실거래가를 가져오기
                근데 얼마나 오래??

        2. 가져와서 평균 가격을 대쉬보드에 표시


'''

'''
Database 모듈들 
'''

# Database configuration
DATABASE_URL = "postgresql://postgres:%40%40N0wsPost@172.16.30.222:5432/apt_db"

# Create SQLAlchemy engine and base
engine = create_engine(DATABASE_URL)
Base = declarative_base()


class ApartmentDeal(Base):
    """Apartment Deals database model"""
    __tablename__ = 'apartment_deals'

    id = Column(Integer, primary_key=True)
    apt_name = Column(String)
    build_year = Column(Integer)
    deal_amount = Column(Float)
    deal_date = Column(Date)
    exclusive_area = Column(Float)
    floor = Column(Integer)
    address_code = Column(String)
    district_name = Column(String)
    jibun = Column(String)



@task
def parse_apt_data(xml_content: str) -> List[Dict]:
    """Parse apartment transaction data from XML content"""
    soup = BeautifulSoup(xml_content, 'xml')
    items = soup.find_all('item')

    parsed_data = []
    for item in items:
        # Convert deal amount from string (e.g., "63,400") to float
        deal_amount = float(item.dealAmount.text.replace(',', ''))

        # Combine deal year, month, and day into a single date
        deal_date = datetime(
            year=int(item.dealYear.text),
            month=int(item.dealMonth.text),
            day=int(item.dealDay.text)
        )

        transaction = {
            'apt_name': item.aptNm.text,
            'build_year': int(item.buildYear.text),
            'deal_amount': deal_amount,
            'deal_date': deal_date,
            'exclusive_area': float(item.excluUseAr.text),
            'floor': int(item.floor.text),
            'address_code': item.sggCd.text,
            'district_name': item.umdNm.text,
            'jibun': item.jibun.text
        }
        parsed_data.append(transaction)

    return parsed_data

@task
def store_transactions(transactions: List[Dict]):
    """Store apartment transactions in PostgreSQL database"""
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for trans_data in transactions:
            transaction = ApartmentDeal(**trans_data)
            session.add(transaction)

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


@flow
def process_apartment_data(xml_content: str):
    """Process apartment data flow"""
    # Create database tables if they don't exist
    Base.metadata.create_all(engine)

    # Parse XML data
    transactions = parse_apt_data(xml_content)

    # Store transactions in database
    store_transactions(transactions)

    return len(transactions)


'''
API 모듈들
'''


@task
def fetch_prices_of_region(region_code: int, year_month: int):
    params = {
        'LAWD_CD': 11110,
        'DEAL_YMD': 201512,
        'serviceKey': 'Kr6RzfG6kX/r4Lyvx/PyV+CnjYlauJH6ciglR8bYlxsqGzn1PKolHN/jE9c1C02rLB+4j5ILB15C4j5tei2pyQ=='
    }

    response = httpx.get('https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade', params=params)
    response.raise_for_status()  # HTTP 에러 체크

    process_apartment_data(response.text)
    #
    # root = ET.fromstring(response.text)
    # ET.indent(root)
    # print ( ET.tostring(root, encoding='unicode') )

    # return response.json()


@flow
def fetch_apt_price():
    cities, regions = fetch_region_codes(1)

    fetch_prices_of_region(11110, 202301)
    # print ('Cities - \n' + json.dumps(cities, indent=2, ensure_ascii=False).encode('utf-8').decode() )
    # print('Regions - \n' + json.dumps(regions, indent=2, ensure_ascii=False).encode('utf-8').decode() )


API_KEY = "0CD21C4C-EE1A-3F46-9F29-D39C4FEAE45C"
BASE_URL = "http://api.vworld.kr/ned/data"
REGION_API_LEVEL = ['admCodeList', 'admSiList', 'amdList']


@task(
    retries=3,
    cache_policy=INPUTS,
    cache_expiration=timedelta(days=30)
)
def fetch_region_info(level: int, region_code: int = 0) -> Optional[list]:
    params = {
        "key": API_KEY,
        "format": "json",
        "numOfRows": 100,  # 최대 결과 수
        "domain": "nowsys.kr/internal.html"
    }

    if level >= len(REGION_API_LEVEL):
        return None

    if level > 0:
        params['admCode'] = region_code

    try:
        response = httpx.get(BASE_URL + '/' + REGION_API_LEVEL[level], params=params)
        response.raise_for_status()  # HTTP 에러 체크
        return response.json()['admVOList']['admVOList']

    except httpx.HTTPError as e:
        print(f"HTTP 요청 오류: {e}")
        raise
    except Exception as e:
        print(f"오류 발생: {e}")
        raise


@flow
def fetch_region_codes(depth: int):
    """
    법정동 정보 가져오기
        vworld.kr를 활용하여 행정구역 정보를 가져옵니다.

    Returns:
        List[Dict]: 행정구역 정보 리스트
    """

    cities = fetch_region_info(0)
    city_codes = [city['admCode'] for city in cities]
    adm_l1 = fetch_region_info.map(1, city_codes).result()

    # print('AdmCode - ' + adm_l1)
    return cities, adm_l1



'''
이 뒤부터 정도된 코드들임. 앞의 코드들은 모두 삭제 예정 
'''


def parse_deals(xml_content: str) -> List[Dict]:
    """Parse apartment transaction data from XML content"""
    soup = BeautifulSoup(xml_content, 'xml')
    items = soup.find_all('item')

    parsed_data = []
    for item in items:
        # Convert deal amount from string (e.g., "63,400") to float
        deal_amount = float(item.dealAmount.text.replace(',', ''))

        # Combine deal year, month, and day into a single date
        deal_date = datetime(
            year=int(item.dealYear.text),
            month=int(item.dealMonth.text),
            day=int(item.dealDay.text)
        )

        transaction = {
            'apt_name': item.aptNm.text,
            'build_year': int(item.buildYear.text),
            'deal_amount': deal_amount,
            'deal_date': deal_date,
            'exclusive_area': float(item.excluUseAr.text),
            'floor': int(item.floor.text),
            'address_code': item.sggCd.text,
            'district_name': item.umdNm.text,
            'jibun': item.jibun.text
        }
        parsed_data.append(transaction)

    return parsed_data


@task
def save_deals(deals: List[Dict]):

    """Store apartment transactions in PostgreSQL database"""
    Session = sessionmaker(bind=engine)
    session = Session()

    try:
        for d in deals:
            deal = ApartmentDeal(**d)
            session.add(deal)

        session.commit()
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

    return len(deals)

@task
def fetch_apt_deals(region_code: int, year_month: int):

    params = {
        'LAWD_CD': region_code,
        'DEAL_YMD': year_month,
        'serviceKey': 'Kr6RzfG6kX/r4Lyvx/PyV+CnjYlauJH6ciglR8bYlxsqGzn1PKolHN/jE9c1C02rLB+4j5ILB15C4j5tei2pyQ=='
    }

    response = httpx.get('https://apis.data.go.kr/1613000/RTMSDataSvcAptTrade/getRTMSDataSvcAptTrade', params=params)
    response.raise_for_status()  # HTTP 에러 체크
    return response.text

# 11110 202503
@flow
def flow_fetch_apt_manually(region_code: str, target_month: str):
    response = fetch_apt_deals(int(region_code), int(target_month))
    deals = parse_deals(response)
    return save_deals(deals)


# Create database tables if they don't exist
Base.metadata.create_all(engine)

if __name__ == '__main__':
    flow_fetch_apt_manually('11110', '202503')

