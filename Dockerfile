FROM apache/airflow:2.10.0-python3.10
COPY requirements.txt /requirements.txt
# Nâng cấp pip
RUN pip install --upgrade pip

# Cài đặt các thư viện từ requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
