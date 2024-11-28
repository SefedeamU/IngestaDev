FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt requirements.txt
RUN pip install -r requirements.txt

COPY . .

CMD ["sh", "-c", "python ingest_service1.py && python ingest_service2.py && python ingest_service3.py && python ingest_service4.py && python ingest_service5.py && python etl_service.py"]