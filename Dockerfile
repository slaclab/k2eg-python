FROM python:3.9
ENV WAIT_VERSION 2.9.0

WORKDIR /opt

RUN mkdir /opt/app
# working directory
WORKDIR /opt/app
COPY . /opt/app/
RUN pip install --no-cache-dir -r requirements.txt
ADD https://github.com/ufoscout/docker-compose-wait/releases/download/$WAIT_VERSION/wait /opt/wait
RUN chmod +x /opt/wait
ENV PATH="${PATH}:/opt:/opt/app"
