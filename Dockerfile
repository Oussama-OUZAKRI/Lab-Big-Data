FROM bitnami/spark:latest

USER root

# Install Python dependencies if needed
COPY requirements.txt /app/
RUN pip install -r /app/requirements.txt

# Copy the application code
COPY structured_streaming_lab.py /app/
WORKDIR /opt/bitnami/spark/work-dir

WORKDIR /app

CMD ["python", "structured_streaming_lab.py"]
