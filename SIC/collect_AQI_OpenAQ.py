import requests
import pandas as pd
import time
from datetime import datetime, timedelta

# Cấu hình API
API_KEY = "94293c1db29d645954592ac27c56c63a5f9f35152eafb15d9a243fff982bc13d"
SENSOR_IDS = {  
    "pm25": 3920,
    "pm10": 3919,
    "co": 3917,
    "no2": 3918,
    "so2": 3920,
    "o3": 4272103,
    "nh3": 4272226
}  # ID của các cảm biến tại Hà Nội
base_url = "https://api.openaq.org/v3/sensors/{sensor_id}/measurements"

# Thời gian cần lấy dữ liệu
start_date = datetime(2025, 5, 1)
end_date = datetime(2025, 5, 31)

# Tạo DataFrame rỗng để lưu dữ liệu
data_dict = {param: [] for param in SENSOR_IDS.keys()}
dates = []

while start_date <= end_date:
    date_str = start_date.strftime("%Y-%m-%dT00:00:00Z")
    next_day = (start_date + timedelta(days=1)).strftime("%Y-%m-%dT23:59:59Z")
    dates.append(start_date.strftime("%Y-%m-%d"))

    for parameter, sensor_id in SENSOR_IDS.items():
        headers = {"X-API-Key": API_KEY}
        params = {
            "datetime_from": date_str,
            "datetime_to": next_day,
            "limit": 1,  # Lấy giá trị gần nhất của ngày
            "order_by": "datetime",
            "sort": "desc"
        }

        url = base_url.format(sensor_id=sensor_id)
        print(f"⬇️ Đang tải {parameter.upper()} cho ngày {start_date.strftime('%Y-%m-%d')}")

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()
        except requests.exceptions.RequestException as e:
            print(f"Lỗi khi tải dữ liệu: {e}")
            data_dict[parameter].append(None)
            continue

        if "results" in data and data["results"]:
            data_dict[parameter].append(data["results"][0]["value"])
        else:
            data_dict[parameter].append(None)

        # 🔹 Nghỉ 1 giây giữa mỗi request để tránh lỗi 429
        time.sleep(1.05)

    start_date += timedelta(days=1)

# Tạo DataFrame chuẩn hóa
df = pd.DataFrame(data_dict, index=dates)

# Lưu dữ liệu vào CSV
df.to_csv("openaq_hanoi_daily_052025.csv")
print("✅ Đã thu thập và lưu dữ liệu thành công vào 'openaq_hanoi_daily_normalized_2024_2025.csv'")

# 94293c1db29d645954592ac27c56c63a5f9f35152eafb15d9a243fff982bc13d