FROM python:3.11-slim

WORKDIR /app

COPY data/generate_data.py .

RUN pip install psycopg2-binary

CMD ["python", "generate_data.py"]
