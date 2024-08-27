FROM python:3.9-slim
# FROM us-central1-docker.pkg.dev/data-poc-1234/dvt-docker-repo/pso-dvt:temp23aug

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True
ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION python

# Oracle dvt setup required arguments
ARG _oracle_instant_client_file_path
ARG _oracle_odbc_version_number

# Copy local code to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY . ./

RUN echo "List current directory --"; ls -ltr

# Install production dependencies.
RUN apt-get update \
    && apt-get install gcc -y \
    && apt-get clean
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

#DB2 Dependencies
RUN pip install ibm_db_sa

# Install gcloud sdk
RUN apt-get update && \
    apt-get install -y curl gnupg && \
    echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] http://packages.cloud.google.com/apt cloud-sdk main" | tee -a /etc/apt/sources.list.d/google-cloud-sdk.list && \
    curl https://packages.cloud.google.com/apt/doc/apt-key.gpg | apt-key --keyring /usr/share/keyrings/cloud.google.gpg  add - && \
    apt-get update -y && \
    apt-get install google-cloud-sdk -y
# RUN pip install "apache-airflow[celery]==2.2.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.3/constraints-3.7.txt"
# RUN pip install apache-airflow-providers-google
# Hive/Impala Dependencies
# RUN pip install hdfs
# RUN pip install thrift-sasl

# Oracle DVT Setup
RUN echo 'Oracle rpm file path and odbc version values are - ' ${_oracle_instant_client_file_path} ${_oracle_odbc_version_number}

RUN chmod u+x ./oracle/oracle_dvt_setup.sh && ./oracle/oracle_dvt_setup.sh ${_oracle_instant_client_file_path} ${_oracle_odbc_version_number}

# Run the web service on container startup. Here we use the gunicorn
# webserver, with one worker process and 8 threads.
# For environments with multiple CPU cores, increase the number of workers
# to be equal to the cores available.
# Timeout is set to 0 to disable the timeouts of the workers to allow Cloud Run to handle instance scaling.
ENV PORT 8080
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 --timeout 0 main:app
