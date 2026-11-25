ARG PYTHON_IMAGE=python:3.12-slim
FROM ${PYTHON_IMAGE}
ENV PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 TZ=Asia/Shanghai
WORKDIR /app
RUN pip install --no-cache-dir aiohttp==3.12.13
COPY exchange_info /app/exchange_info
COPY step2/*.py step2/settings.json /app/step2/
COPY step2/collectors /app/step2/collectors
COPY step2/reference /app/step2/reference
CMD ["python", "/app/step2/01_collect.py", "--help"]
