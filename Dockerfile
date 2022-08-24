FROM python:3.8-slim

WORKDIR /app

COPY ./requirements.txt /app/

RUN apt update && apt install -y python3-pip && \
    pip install -r /app/requirements.txt

COPY . /app
RUN chmod a+x /app/run.sh

ENTRYPOINT ["/app/run.sh"]

