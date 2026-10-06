FROM python:3.11.15-slim@sha256:90744cff8f32887f075c47d747a173ff333e9e98801667af93c357fa9f5e28ff AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1 HOME=/home/app
WORKDIR /app
COPY requirements.txt .
RUN python -m pip install --no-deps -r requirements.txt && python -m pip check && \
    groupadd --gid 10001 app && useradd --uid 10001 --gid app --create-home app && \
    mkdir /app/tmp && chown app:app /app/tmp
COPY portfolio_backend/ portfolio_backend/
COPY data/ data/
COPY data_sources.py mobile_api.py web_dashboard.py ./
COPY scripts/import_option_market_history.py scripts/import_option_market_history.py
ARG APP_BUILD_VERSION=development
ENV APP_BUILD_VERSION=${APP_BUILD_VERSION}
USER 10001:10001
CMD ["sh", "-c", "exec uvicorn mobile_api:app --host 0.0.0.0 --port ${PORT:-8080}"]

FROM runtime AS test
ENV GOOGLE_APPLICATION_CREDENTIALS=/tmp/nonexistent-test-credentials.json
USER root
COPY requirements-dev.txt requirements-streamlit.txt ./
RUN python -m pip install --no-deps -r requirements-dev.txt && python -m pip check
COPY tests/ tests/
COPY scripts/deploy_verified.py scripts/ibkr_backfill.py scripts/
COPY streamlit_app.py visual_prototype.py ./
USER 10001:10001
CMD ["python", "-m", "pytest", "-q", "-p", "no:cacheprovider"]

FROM runtime AS production
