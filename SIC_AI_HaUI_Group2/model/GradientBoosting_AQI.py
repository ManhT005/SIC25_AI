import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.metrics import mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns

# 1. Đọc dữ liệu
df = pd.read_csv("dataset.csv")

# 2. Tiền xử lý dữ liệu (tùy vào cấu trúc dataset của bạn)
df = df.dropna()  # loại bỏ các dòng có giá trị null

# Giả sử: cột mục tiêu là 'temperature' (nhiệt độ) hoặc 'pm25', bạn cần điều chỉnh theo cột thực tế
target_column = 'temperature'  # hoặc 'pm25', 'humidity', etc.
X = df.drop(columns=[target_column])
y = df[target_column]

# Nếu có cột ngày giờ -> bỏ đi hoặc xử lý datetime riêng
for col in X.columns:
    if X[col].dtype == 'object':
        try:
            X[col] = pd.to_datetime(X[col])
            X[col] = X[col].map(pd.Timestamp.toordinal)
        except:
            X[col] = pd.factorize(X[col])[0]

# 3. Chia tập train-test
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42
)

# 4. Huấn luyện mô hình Gradient Boosting
model = GradientBoostingRegressor(
    n_estimators=200,
    learning_rate=0.1,
    max_depth=4,
    subsample=0.8,
    random_state=42
)
model.fit(X_train, y_train)

# 5. Dự đoán & đánh giá
y_pred = model.predict(X_test)
rmse = mean_squared_error(y_test, y_pred, squared=False)
r2 = r2_score(y_test, y_pred)

print(f"RMSE: {rmse:.2f}")
print(f"R² Score: {r2:.4f}")

# 6. Biểu đồ: so sánh giá trị thực tế và dự đoán
plt.figure(figsize=(10, 6))
sns.scatterplot(x=y_test, y=y_pred, alpha=0.7)
plt.xlabel("Giá trị thực tế")
plt.ylabel("Dự đoán")
plt.title(f"Dự đoán {target_column} bằng Gradient Boosting")
plt.plot([y.min(), y.max()], [y.min(), y.max()], 'r--')
plt.grid(True)
plt.show()
