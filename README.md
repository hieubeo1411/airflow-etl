# Project ETL Pipeline

## 1. Cài đặt môi trường

  - Cài đặt Python 3.10 trở lên
  - Cài đặt Docker.
  - Cài đặt các dependencies cho airflow bằng Dockerfile.
    pip install -r requirements.txt
  
## 2. Chạy chương trình:
  - Sử dụng docker-compose để tự động cài đặt và cấu hình MinIO, Airflow, webapp chứa data được customize, và thông tin đăng nhập của chúng.
  - Chạy trên terminal của IDE: docker compose up để thiết lập container và image đồng thời chạy chương trình trên airflow.
## 3. Mô tả:
  - Khởi tạo một webapp,py local chứa dữ liệu của người dùng được customize theo ngày. ( Có Dockerfile riêng để import thư viện ).
  - Viết data pineline để chạy trong file get_data_from_web_app.py. Chi tiết chạy theo thứ tự sau: get_connection >> calculate_stats >> upload_file
      1. get_connection: kết nối tới webapp.
      2. calculate_stats: tải data về và chuyển sang dạng csv và nén sang dạng gzip.
      3. upload_file: upload file lên Minio (cần phải đăng nhập vào Minio local và tạo access_key và secret_key để đẩy lên bucket)
  
    
  
