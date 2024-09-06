FROM python:3.10
WORKDIR ./

COPY requirements.txt .
RUN pip install -r requirements.txt && rm -f requirements.txt

COPY app.py .

EXPOSE 5000

CMD python app.py
