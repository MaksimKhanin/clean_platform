# Base Image
FROM python:3.8-slim

# Arguments that can be set with docker build
ARG AIRFLOW_VERSION=1.10.14
ARG AIRFLOW_HOME=/usr/local/airflow
ARG PYTHON_VERSION=3.8

# Export the environment variable AIRFLOW_HOME where airflow will be installed
ENV AIRFLOW_HOME=${AIRFLOW_HOME}

# Install dependencies and tools
RUN apt-get update -yqq && \
    apt-get upgrade -yqq && \
    apt-get install -yqq --no-install-recommends \ 
    wget \
    libczmq-dev \
    curl \
    libssl-dev \
    git \
    inetutils-telnet \
    bind9utils freetds-dev \
    libkrb5-dev \
    libsasl2-dev \
    libffi-dev libpq-dev \
    freetds-bin build-essential \
    default-libmysqlclient-dev \
    apt-utils \
    rsync \
    zip \
    unzip \
    gcc \
    vim \
    locales \
    && apt-get clean

COPY ./constraints-3.8.txt /constraints-3.8.txt

# Upgrade pip
# Create airflow user 
# Install apache airflow with subpackages
RUN pip install --upgrade pip && \
    useradd -ms /bin/bash -d ${AIRFLOW_HOME} airflow && \
    pip install apache-airflow[password,slack,crypto,postgres,docker]==${AIRFLOW_VERSION} --constraint /constraints-3.8.txt


# packages needed for dags
RUN pip install python-dotenv && \
    pip install flask-bcrypt && \
    pip install scikit-learn

RUN apt-get install gettext-base

# cfgs needed for dags
COPY ./airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
RUN chmod 777 ${AIRFLOW_HOME}/airflow.cfg

# Copy the entrypoint.sh from host to container (at path AIRFLOW_HOME)
COPY ./entrypoint.sh ./entrypoint.sh

# Set the entrypoint.sh file to be executable
RUN chmod +x ./entrypoint.sh

# Set the owner of the files in AIRFLOW_HOME to the user airflow
RUN chown -R airflow: ${AIRFLOW_HOME}

# Set the username to use
USER airflow

# Set workdir (it's like a cd inside the container)
WORKDIR ${AIRFLOW_HOME}

COPY ./cfg_parser.py ./cfg_parser.py
# Create the dags folder which will contain the DAGs
RUN mkdir dags

# Expose ports (just to indicate that this container needs to map port)
EXPOSE 8080

# Execute the entrypoint.sh

ENTRYPOINT /entrypoint.sh