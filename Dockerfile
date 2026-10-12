
FROM apache/spark:4.1.3-scala2.13-java17-python3-ubuntu

USER root

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY app/ /app/app/

WORKDIR /app/app

CMD ["sleep", "infinity"]
