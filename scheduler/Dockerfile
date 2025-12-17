FROM apache/airflow:3.0.6

ARG APPTAINER_VERSION=1.4.2

# install Apptainer
USER root
WORKDIR /opt

# librairies requises avant installation
RUN apt-get update && apt-get install -y \
    ca-certificates cpio curl e2fsprogs fuse-overlayfs \
    fuse3 rpm2cpio squashfuse uidmap && \
    rm -rf /var/lib/apt/lists/*

# Script officiel unprivileged et ranges subuid/subgid pour le user airflow
RUN curl -proto "=https" --tlsv1.2 -fsSL https://raw.githubusercontent.com/apptainer/apptainer/main/tools/install-unprivileged.sh \
    | bash -s - -v ${APPTAINER_VERSION} /opt/apptainer && \
    ln -sf /opt/apptainer/bin/apptainer /usr/local/bin/apptainer && \
    ln -sf /usr/local/bin/apptainer /usr/local/bin/singularity || true && \ 
    grep -q '^airflow:' /etc/subuid || echo 'airflow:100000:65536' >> /etc/subuid ; \
    grep -q '^airflow:' /etc/subgid || echo 'airflow:100000:65536' >> /etc/subgid

# airflow requirement
USER airflow
WORKDIR /opt/airflow

COPY requirements.txt .

RUN pip install -r requirements.txt
