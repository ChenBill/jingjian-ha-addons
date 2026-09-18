FROM python:3.12-slim-bookworm

ENV PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r ./requirements.txt

COPY sequence_gateway.py ./sequence_gateway.py
COPY run.sh ./run.sh
RUN chmod +x ./run.sh

CMD ["/app/run.sh"]
