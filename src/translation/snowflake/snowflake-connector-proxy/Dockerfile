# https://hub.docker.com/_/python
FROM python:3.11-slim

# Allow statements and log messages to immediately appear in the logs
ENV PYTHONUNBUFFERED True

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN apt-get -y update && apt-get -y install curl
# Copy snowflake connector jar
RUN curl https://storage.googleapis.com/dmt-snowflake/snowflake-to-bq-data-transfer-1.0.0.jar -o ./jar/snowflake-to-bq-data-transfer.jar

# Install production dependencies.
RUN pip install --no-cache-dir -r requirements.txt

ENV SECRET_KEY RANDOM0XXXXXXXXXXXXXXXXXXXXXXXXX

# Install Java 11
# RUN apt-get -y install software-properties-common
# RUN add-apt-repository ppa:openjdk-r/ppa
# RUN apt-get -y install openjdk-11-jdk

COPY --from=openjdk:11-jre-slim /usr/local/openjdk-11 /usr/local/openjdk-11
ENV JAVA_HOME /usr/local/openjdk-11
ENV PATH $JAVA_HOME/bin:$PATH

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:APP