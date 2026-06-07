# Custom container for Dataflow (Storage Write API + GCS ingestion pipeline).
# The base image ships the Beam SDK and the /opt/apache/beam/boot launcher that
# Dataflow requires. Python version here must match --sdk_container_image tag
# passed at submit time and the version used to run pipeline.py locally.
FROM apache/beam_python3.12_sdk:2.69.0

# Install extra dependencies on top of what the base image already ships.
# apache-beam[gcp] is already present; listing it here pins the version so pip
# will upgrade or downgrade to match if requirements.txt is later version-pinned.
COPY requirements.txt /pipeline/requirements.txt
RUN pip install --no-cache-dir -r /pipeline/requirements.txt

# Copy the entry point and the python_scripts package.
# /pipeline is on PYTHONPATH so `from python_scripts.X import Y` resolves correctly
# on both the main worker and any side-input / expand workers.
COPY pipeline.py        /pipeline/pipeline.py
# COPY python_scripts/    /pipeline/python_scripts/

ENV PYTHONPATH="/pipeline"
WORKDIR /pipeline

# Required by Dataflow — do not change.
ENTRYPOINT ["/opt/apache/beam/boot"]
