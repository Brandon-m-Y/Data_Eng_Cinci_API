# Batch image for the Cloud Run Job. The mode is passed as args, e.g.
#   docker run --env-file .env <image> --delta
# Base pinned by digest (python:3.13-slim, Python 3.13.15, pulled 2026-09-19)
# so a rebuild gets the same OS layer; bump it deliberately.
FROM python:3.13-slim@sha256:8d9d0b8bcf6506481eae4907c18f5e3e7902e629f5f6d684f9e7c32e85e3ddf0

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first so code-only changes reuse this layer. constraints.txt
# pins every transitive package to the tested versions.
COPY requirements.txt constraints.txt ./
RUN pip install -r requirements.txt -c constraints.txt

# SQL files are opened relative to WORKDIR (SQL_FILE / PANEL_SQL_FILE)
COPY Run_Pipeline.py Get_Data.py Load_to_GBQ.py Pipeline_Config.py \
     Star_Schema_ETL.sql ML_Crash_Panel.sql ./

RUN useradd --create-home --uid 1000 etl
USER etl

ENTRYPOINT ["python", "Run_Pipeline.py"]
CMD ["--delta"]
