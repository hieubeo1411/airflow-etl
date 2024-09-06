from airflow import DAG
from datetime import datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pandas as pd
from minio import Minio
from minio.error import S3Error
import os


# Hàm tính toán thống kê
def caculate_stats(input_path, output_path):
    df = pd.read_json(input_path)
    df = df.groupby(["date", "user"]).size().reset_index()
    df.to_csv(output_path)


# Hàm đẩy file CSV lên MinIO
def upload_to_minio(output_path):
    # Khởi tạo kết nối MinIO
    minio_client = Minio(
        "localhost:9000",  # Địa chỉ MinIO
        access_key="dyZdgks2xg4iYOBtbh6K",  # Thay đổi access key phù hợp
        secret_key="8gVq46Cm1BNItYhUpHiuu2qXHcmfMC5Ir4niAw9C",  # Thay đổi secret key phù hợp
        secure=False
    )

    bucket_name = "test"  # Tên bucket bạn muốn đẩy file lên

    # Tạo bucket nếu chưa tồn tại
    if not minio_client.bucket_exists(bucket_name):
        minio_client.make_bucket(bucket_name)

    # Đẩy file CSV lên MinIO
    try:
        file_name = os.path.basename(output_path)
        minio_client.fput_object(bucket_name, file_name, output_path)
        print(f"Successfully uploaded {file_name} to {bucket_name}.")
    except S3Error as err:
        print(f"Error occurred: {err}")


# Cấu hình DAG
with DAG(
        dag_id="get_data",
        start_date=datetime(2024, 8, 27),
        end_date=datetime(2024, 9, 1),
        schedule_interval="@daily"
) as dag:
    # Task kết nối và lấy dữ liệu từ web app
    get_connection = BashOperator(
        task_id="get_connection",
        bash_command='curl -o /var/tmp/events-{{ds}}.json http://airflow-etl-app-container-1:5000/events?start_date={{ds}}&end_date={{next_ds}}',
    )

    # Task tính toán thống kê
    calculate_stats = PythonOperator(
        task_id='statistic',
        python_callable=caculate_stats,
        op_kwargs={'input_path': '/var/tmp/events-{{ds}}.json', 'output_path': '/var/tmp/events-{{ds}}.csv'}
    )

    # Task đẩy file CSV lên MinIO
    upload_file = PythonOperator(
        task_id='upload_to_minio',
        python_callable=upload_to_minio,
        op_kwargs={'output_path': '/var/tmp/events-{{ds}}.csv'}
    )

# Xác định thứ tự chạy của các task
get_connection >> calculate_stats >> upload_file
