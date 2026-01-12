FROM python:3.9-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app/s3_copy_job.py /app/job.py
COPY docker/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh /app/job.py
RUN mkdir -p /logs
ENTRYPOINT ["/entrypoint.sh"]
