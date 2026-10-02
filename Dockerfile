# Batch image for the Cloud Run Job. The mode is passed as args, e.g.
#   docker run --env-file .env <image> --delta
# Base pinned by digest (python:3.13-slim, Python 3.13.16, Debian 13.7,
# pulled 2026-10-01) so every rebuild gets the same OS layer. The tag alone
# would drift under us; the digest makes a build reproducible.
#
# To bump: docker pull python:3.13-slim
#          docker image inspect python:3.13-slim --format '{{index .RepoDigests 0}}'
# Paste that digest below, rebuild, and re-run the integration suite inside
# the image -- the interpreter and OS packages change, the pinned Python
# dependencies in constraints.txt do not.
FROM python:3.13-slim@sha256:8296499feed1c18bd8064c279d45e2a1b4b6be586f8b9e16dcf2aaf843480d88

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
