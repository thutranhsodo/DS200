import numpy as np
import time
import socket

def generate_data():
    feature1 = np.random.uniform(0, 10)
    feature2 = np.random.uniform(0, 5)
    noise = np.random.normal(0, 0.2)
    target = 2.5 * feature1 + 1.8 * feature2 + noise
    return f"{feature1:.3f},{feature2:.3f},{target:.3f}"

def run_streaming_server(host='127.0.0.1', port=9999):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.bind((host, port))
        server.listen()
        print(f"\nServer phát dữ liệu đang chạy tại host: {host} - port: {port}")

        while True:
            client_conn, client_addr = server.accept()
            print(f"\nĐã kết nối với client tại {client_addr}")
            with client_conn:
                try:
                    while True:
                        sample_data = generate_data()
                        client_conn.sendall((sample_data + '\n').encode())
                        print(f"Gửi: {sample_data}")
                        time.sleep(0.5)
                except (ConnectionResetError, ConnectionAbortedError, BrokenPipeError):
                    print("\nClient đã ngắt kết nối. Dừng phiên làm việc...")
                    break