FROM python:3.8-slim

WORKDIR /app

RUN apt-get update \
 && apt-get install -y wget gnupg lsb-release \
 && echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" \
    > /etc/apt/sources.list.d/pgdg.list \
 && wget --quiet -O - https://www.postgresql.org/media/keys/ACCC4CF8.asc | apt-key add - \
 && apt-get update \
 && apt-get install -y postgresql-client-18 \
 && rm -rf /var/lib/apt/lists/*

COPY elt_script.py .

CMD ["python", "elt_script.py"]