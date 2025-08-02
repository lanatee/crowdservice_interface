import os
import requests
import datetime
import xml.etree.ElementTree as ET
import psycopg2
from dotenv import load_dotenv

load_dotenv()

# 환경변수
KMA_API_KEY = os.getenv("KMA_API_KEY")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "wt_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "admin123")

def get_kst_now():
    return datetime.datetime.utcnow() + datetime.timedelta(hours=9)

def get_safe_base_time():
    now = get_kst_now()

    # 항상 1시간 전 기준 시각으로 요청
    target_time = now - datetime.timedelta(hours=1)
    base_date = target_time.strftime("%Y%m%d")
    base_time = target_time.strftime("%H00")
    return base_date, base_time

def fetch_weather(base_date, base_time, nx=55, ny=127):
    url = "http://apis.data.go.kr/1360000/VilageFcstInfoService_2.0/getUltraSrtNcst"
    params = {
        "serviceKey": "DUZ7d6Gh1UXHBGbd5/MYj0DXqppwedSd3GCDWclNv5UyfCdpfoUicuVgdD+N+ESsb+q1TiW14UXyJ9v2hGy3FQ==",
        "pageNo": "1",
        "numOfRows": "1000",
        "dataType": "XML",
        "base_date": base_date,
        "base_time": base_time,
        "nx": str(nx),
        "ny": str(ny)
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        print(f"📡 요청 URL: {response.url}")  # 전체 URL 출력

        if response.status_code != 200:
            print(f"❌ API 호출 실패 [{response.status_code}]")
            print(f"🔍 응답 내용: {response.text[:500]}")  # 너무 길 경우 일부만
            return None

        root = ET.fromstring(response.content)
        items = root.findall(".//item")
        if not items:
            print("⚠️ 응답 성공했지만 데이터 항목 없음")
            return None

        data = {
            "base_date": base_date,
            "base_time": base_time,
            "nx": nx,
            "ny": ny
        }
        for item in items:
            category = item.findtext("category")
            value = item.findtext("obsrValue")
            data[category] = value

        return data

    except requests.exceptions.RequestException as e:
        print("🚨 네트워크 또는 API 예외 발생:")
        print("⛔", str(e))
        return None


def save_to_db(data):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cur = conn.cursor()
    cur.execute("""
        CREATE TABLE IF NOT EXISTS current (
            id SERIAL PRIMARY KEY,
            base_date VARCHAR(8),
            base_time VARCHAR(4),
            nx INTEGER,
            ny INTEGER,
            t1h REAL,
            reh REAL,
            rn1 REAL,
            pty INTEGER,
            wsd REAL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

    cur.execute("""
        INSERT INTO current
        (base_date, base_time, nx, ny, t1h, reh, rn1, pty, wsd)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        data.get("base_date"),
        data.get("base_time"),
        data.get("nx"),
        data.get("ny"),
        float(data.get("T1H", 0)),
        float(data.get("REH", 0)),
        float(data.get("RN1", 0)),
        int(data.get("PTY", 0)),
        float(data.get("WSD", 0))
    ))

    conn.commit()
    cur.close()
    conn.close()
    print("✅ 날씨 저장 완료:", data["base_date"], data["base_time"])

def main():
    base_date, base_time = get_safe_base_time()
    data = fetch_weather(base_date, base_time)
    if data:
        save_to_db(data)
    else:
        print("❌ 저장할 날씨 데이터 없음")

if __name__ == "__main__":
    main()
