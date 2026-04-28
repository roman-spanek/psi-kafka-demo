FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1

WORKDIR /app

COPY services/reports/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY services/reports /app

CMD ["python", "app.py"]

