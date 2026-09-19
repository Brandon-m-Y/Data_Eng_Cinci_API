# Batch image for the Cloud Run Job. The mode is passed as args, e.g.
#   docker run --env-file .env <image> --delta
FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Dependencies first so code-only changes reuse this layer
COPY requirements.txt .
RUN pip install -r requirements.txt

# SQL files are opened relative to WORKDIR (SQL_FILE / PANEL_SQL_FILE)
COPY Run_Pipeline.py Get_Data.py Load_to_GBQ.py Star_Schema_ETL.sql ML_Crash_Panel.sql ./

RUN useradd --create-home --uid 1000 etl
USER etl

ENTRYPOINT ["python", "Run_Pipeline.py"]
CMD ["--delta"]
