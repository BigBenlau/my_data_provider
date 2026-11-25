ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 TZ=Asia/Shanghai
WORKDIR /app
RUN pip install --no-cache-dir aiohttp==3.12.13
COPY exchange_info /app/exchange_info
COPY step2/common /app/step2/common
COPY step2/01_collector /app/step2/01_collector
COPY step2/02_normalize_tardis /app/step2/02_normalize_tardis
COPY step2/03_compare /app/step2/03_compare
CMD ["python", "/app/step2/01_collector/01_collect.py", "--help"]
