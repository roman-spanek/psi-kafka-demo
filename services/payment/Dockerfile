FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/payment/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/payment /app

CMD ["python", "app.py"]

