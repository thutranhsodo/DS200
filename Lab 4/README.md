# Real-Time Machine Learning with PySpark Streaming

Dự án trình bày cách huấn luyện mô hình học máy Random Forest Regressor theo thời gian thực với dữ liệu được gửi qua socket, sử dụng Apache Spark (PySpark) và Python.

## Mô tả

- Một server socket phát sinh dữ liệu giả lập (`feature1`, `feature2`, `target`) được tính toán từ một công thức tuyến tính có thêm nhiễu.
- Dữ liệu được gửi tới Spark Structured Streaming.
- PySpark xử lý dữ liệu, huấn luyện mô hình học máy (Random Forest) theo từng lô (`batch`).
- Sau mỗi batch, mô hình được đánh giá bằng RMSE (Root Mean Squared Error).

## Mô hình

- Mặc định sử dụng **Random Forest Regressor** từ `pyspark.ml.regression`.

## Cấu trúc thư mục
- data_sender.py: Phát sinh dữ liệu và gửi qua socket 
- data_receiver.py: Nhận dữ liệu, huấn luyện mô hình
- main.py: Khởi động server và client song song

