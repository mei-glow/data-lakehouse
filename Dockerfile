FROM apache/airflow:3.1.6
ADD requirements.txt requirements.txt
RUN pip install -r requirements.txt