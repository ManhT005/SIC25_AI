import pandas as pd

# ======== 1. Đọc dữ liệu từ file gốc ========
df = pd.read_csv(r"collected_data.csv")
#df = df.rename(columns={"Unnamed: 0": "date"})  # Đổi tên cột ngày cho rõ ràng


cols_to_fill = ['pm25', 'pm10', 'co', 'no2', 'so2', 'o3']
for col in cols_to_fill:
    df[col] = df[col].fillna(df[col].rolling(window=5, min_periods=1, center=True).mean(), inplace=True)

# ======== 2. Định nghĩa breakpoint & hàm tính AQI ========
def calc_aqi(conc, bp):
    for c_low, c_high, i_low, i_high in bp:
        if c_low <= conc <= c_high:
            return round((i_high - i_low) / (c_high - c_low) * (conc - c_low) + i_low)
    return None

bp_pm25 = [(0.0, 12.0, 0, 50), (12.1, 35.4, 51, 100), (35.5, 55.4, 101, 150),
           (55.5, 150.4, 151, 200), (150.5, 250.4, 201, 300), (250.5, 350.4, 301, 400),
           (350.5, 500.4, 401, 500)]

bp_pm10 = [(0, 54, 0, 50), (55, 154, 51, 100), (155, 254, 101, 150),
           (255, 354, 151, 200), (355, 424, 201, 300), (425, 504, 301, 400),
           (505, 604, 401, 500)]

bp_co = [(0.0, 4.4, 0, 50), (4.5, 9.4, 51, 100), (9.5, 12.4, 101, 150),
         (12.5, 15.4, 151, 200), (15.5, 30.4, 201, 300), (30.5, 40.4, 301, 400),
         (40.5, 50.4, 401, 500)]

bp_so2 = [(0, 35, 0, 50), (36, 75, 51, 100), (76, 185, 101, 150),
          (186, 304, 151, 200), (305, 604, 201, 300), (605, 804, 301, 400),
          (805, 1004, 401, 500)]

bp_no2 = [(0, 53, 0, 50), (54, 100, 51, 100), (101, 360, 101, 150),
          (361, 649, 151, 200), (650, 1249, 201, 300), (1250, 1649, 301, 400),
          (1650, 2049, 401, 500)]

bp_o3 = [(0.000, 0.054, 0, 50), (0.055, 0.070, 51, 100), (0.071, 0.085, 101, 150),
         (0.086, 0.105, 151, 200), (0.106, 0.200, 201, 300), (0.201, 0.404, 301, 500)]

# ======== 3. Tính AQI cho từng chất ========
df["aqi_pm25"] = df["pm25"].apply(lambda x: calc_aqi(x, bp_pm25))
df["aqi_pm10"] = df["pm10"].apply(lambda x: calc_aqi(x, bp_pm10))
df["aqi_co"]   = df["co"].apply(lambda x: calc_aqi(x, bp_co))
df["aqi_no2"]  = df["no2"].apply(lambda x: calc_aqi(x * 1000, bp_no2))  # ppm → ppb
df["aqi_so2"]  = df["so2"].apply(lambda x: calc_aqi(x, bp_so2))
df["aqi_o3"]   = df["o3"].apply(lambda x: calc_aqi(x, bp_o3))

# ======== 4. Tính AQI tổng hợp mỗi ngày ========
df["AQI"] = df[["aqi_pm25", "aqi_pm10", "aqi_co", "aqi_no2", "aqi_so2", "aqi_o3"]].max(axis=1)

df.drop(columns=[
    "aqi_pm25", "aqi_pm10", "aqi_co", "aqi_no2", "aqi_so2", "aqi_o3"  # AQI từng chất
], inplace=True)

# ======== 5. Ghi kết quả ra file mới ========
df.to_csv(r"dataset.csv", index=False)