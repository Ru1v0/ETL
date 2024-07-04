FROM apache/airflow:2.8.1

USER root

# Install required packages
RUN apt-get update && apt-get install -y locales \
    && rm -rf /var/lib/apt/lists/*

# Configure locale to pt_BR.UTF-8
RUN localedef -i pt_BR -c -f UTF-8 -A /usr/share/locale/locale.alias pt_BR.UTF-8
ENV LANG pt_BR.UTF-8
ENV LANGUAGE pt_BR:pt:en
ENV LC_ALL pt_BR.UTF-8

# Set the PYTHONPATH
ENV PYTHONPATH "${PYTHONPATH}:/opt/airflow"
ENV PYTHONPATH "${PYTHONPATH}:/home/opc/app-2"

# Oracle Client environment settings
ENV ORACLE_HOME=/opt/oracle/instantclient_19_3
ENV LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$ORACLE_HOME

# Copy requirements and wallet for Oracle
COPY requirements.txt ./requirements.txt
COPY docs/cwallet.zip /cwallet.zip

# Install system dependencies
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
         curl unzip libxcb1 python3 python3-dev \
         wget apt-utils python3-pip build-essential \
         libaio1 libgbm1 libpci3 libnss3 \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*


# Download and install Oracle Client
RUN mkdir -p /opt/oracle; \
    cd /opt/oracle/;  \
    wget https://download.oracle.com/otn_software/linux/instantclient/193000/instantclient-basic-linux.x64-19.3.0.0.0dbru.zip;  \
    unzip instantclient-basic-linux.x64-19.3.0.0.0dbru.zip; \
    rm instantclient-basic-linux.x64-19.3.0.0.0dbru.zip; \
    sh -c "echo /opt/oracle/instantclient_19_3 > /etc/ld.so.conf.d/oracle-instantclient.conf";  \
    ldconfig; \
    cd /opt/oracle/instantclient_19_3/network/admin/; \
    unzip /cwallet.zip;


# Copy your application code
COPY . /opt/airflow
WORKDIR /opt/airflow

# Switch to user airflow for security
USER airflow

# Set JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Install Airflow and other Python dependencies
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" 
RUN pip install --no-cache-dir -r requirements.txt

# Set default command to run Airflow
ENV SHELL /bin/bash
CMD ["airflow", "webserver"]