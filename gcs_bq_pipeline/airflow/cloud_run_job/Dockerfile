# Build from the cloud_run_job/ directory.
# python_scripts/ must be copied here first by the deployment step — see gcp_deployment.txt.
#
#   gcloud builds submit . --tag=IMAGE

FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY python_scripts/ ./python_scripts/
COPY main.py .

ENV PYTHONPATH="/app"

CMD ["python", "main.py"]
