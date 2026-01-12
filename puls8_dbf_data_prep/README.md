# puls8_dbf_data_prep

Production-ready S3 COPY-only data preparation job.

## Key Characteristics
- COPY only (no delete)
- Timestamped destination folders
- Schedule controlled externally (cron)
- Dockerized execution
- IAM Role based security

## Execution
docker build -t puls8_dbf_data_prep:latest .
docker run --rm -v /home/ec2-user/jobs/logs:/logs puls8_dbf_data_prep:latest
