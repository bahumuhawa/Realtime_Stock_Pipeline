FROM python:3.11-slim AS python

# system deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# copy code
COPY . /app

# mark final stage as python runtime
FROM python AS final
WORKDIR /app
COPY --from=python /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=python /app /app

ENV PYTHONUNBUFFERED=1

# define a default target but we override commands in compose
ENTRYPOINT ["sh", "-c"]
