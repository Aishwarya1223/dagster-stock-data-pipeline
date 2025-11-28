FROM python:3.11-slim

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    curl \
    git \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/dagster

COPY repo/ requirements.txt ./
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN mkdir -p /opt/dagster/app /opt/dagster_home
COPY repo/ /opt/dagster/app
COPY workspace.yaml /opt/dagster/app/workspace.yaml

ENV PATH="/root/.local/bin:${PATH}"

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "/opt/dagster/app/workspace.yaml"]
