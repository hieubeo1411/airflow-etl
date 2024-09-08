from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from concurrent.futures import ThreadPoolExecutor
import pandas as pd
from minio import Minio
from minio.error import S3Error
import os
import gzip
import shutil


# Hàm xử lý dữ liệu của một nhóm con
def process_group(group):
    return group.groupby(["date", "user"]).size().reset_index()


# Hàm tính toán thống kê với multi-threading và nén file CSV
def caculate_stats(input_path, output_path):
    df = pd.read_json(input_path)

    # Chia dataframe thành các phần nhỏ hơn để xử lý song song
    groups = [group for _, group in df.groupby(df.index // 1000)]  # Chia dữ liệu thành các khối 1000 dòng

    # Sử dụng ThreadPoolExecutor để xử lý các nhóm song song
    with ThreadPoolExecutor() as executor:
        results = list(executor.map(process_group, groups))

    # Kết hợp kết quả từ các thread
    final_df = pd.concat(results)

    # Lưu kết quả cuối cùng vào CSV
    csv_output_path = output_path + ".csv"
    final_df.to_csv(csv_output_path, index=False)

    # Nén file CSV bằng gzip
    compressed_output_path = output_path + ".csv.gz"
    with open(csv_output_path, 'rb') as f_in:
        with gzip.open(compressed_output_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

    # Xóa file CSV gốc sau khi đã nén
    os.remove(csv_output_path)

    return compressed_output_path


# Hàm đẩy file nén lên MinIO với multi-threading
def upload_to_minio(output_path):
    # Khởi tạo kết nối MinIO
    minio_client = Minio(
        "minio:9000",  # URL của Minio server
        access_key="HtcHhNvx8xz3Gr9QO4IE",
        secret_key="Yj2l0PMzEG7tQPFKVpeDDAQsFbiB0SDRqK24e8wX",
        secure=False
    )

    bucket_name = "test2"  # Tên bucket bạn muốn đẩy file lên

    # Tạo bucket nếu chưa tồn tại
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Kích thước tối đa mỗi phần nhỏ (ví dụ: 100 MB)
    chunk_size = 100 * 1024 * 1024

    def upload_chunk(chunk_path, chunk_name):
        minio_client.fput_object(bucket_name, chunk_name, chunk_path)
        print(f"Successfully uploaded {chunk_name}.")

    # Chia nhỏ file nếu kích thước lớn
    file_size = os.path.getsize(output_path)
    file_name = os.path.basename(output_path)

    if file_size > chunk_size:
        with open(output_path, 'rb') as f:
            part_num = 0
            while True:
                chunk_data = f.read(chunk_size)
                if not chunk_data:
                    break
                part_num += 1
                chunk_file = f"/tmp/{file_name}.part{part_num}"
                with open(chunk_file, 'wb') as chunk:
                    chunk.write(chunk_data)

                # Sử dụng multi-threading để upload các phần song song
                with ThreadPoolExecutor() as executor:
                    executor.submit(upload_chunk, chunk_file, f"{file_name}.part{part_num}")

                # Xóa phần nhỏ sau khi đã upload để tiết kiệm không gian
                os.remove(chunk_file)
    else:
        # Nếu file nhỏ hơn giới hạn chunk, upload trực tiếp
        minio_client.fput_object(bucket_name, file_name, output_path)
        print(f"Successfully uploaded {file_name} to {bucket_name}.")

    # Xóa file nén gốc sau khi upload
    os.remove(output_path)


# Cấu hình DAG
with DAG(
        dag_id="get_data",
        start_date=datetime(2024, 8, 27),
        end_date=datetime(2024, 9, 1),
        schedule_interval='0 7 * * *'
) as dag:
    # Task kết nối và lấy dữ liệu từ web app
    get_connection = BashOperator(
        task_id="get_connection",
        bash_command='curl -o /var/tmp/events-{{ds}}.json http://airflow-etl-app-container-1:5000/events?start_date={{ds}}&end_date={{next_ds}}',
    )

    # Task tính toán thống kê và nén
    calculate_stats = PythonOperator(
        task_id='statistic',
        python_callable=caculate_stats,
        op_kwargs={'input_path': '/var/tmp/events-{{ds}}.json', 'output_path': '/var/tmp/events-{{ds}}'}
    )

    # Task đẩy file CSV nén lên MinIO
    upload_file = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
        op_kwargs={'output_path': '/var/tmp/events-{{ds}}.csv.gz'}
    )

# Xác định thứ tự chạy của các task
get_connection >> calculate_stats >> upload_file
