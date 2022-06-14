FROM flink:1.15.0-scala_2.12

ADD ./code /opt/flink/code

ADD ./lib /opt/flink/lib

RUN apt-get update -y && \
    apt-get install -y build-essential libssl-dev zlib1g-dev libbz2-dev libffi-dev && \
    wget https://www.python.org/ftp/python/3.7.9/Python-3.7.9.tgz && \
    tar -xvf Python-3.7.9.tgz && \
    cd Python-3.7.9 && \
    ./configure --without-tests --enable-shared && \
    make -j6 && \
    make install && \
    ldconfig /usr/local/lib && \
    cd .. && rm -f Python-3.7.9.tgz && rm -rf Python-3.7.9 && \
    ln -s /usr/local/bin/python3 /usr/local/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install python3-pip -y

RUN pip3 install google

RUN python3 -m venv /opt/pyflink && \
    cd /opt/pyflink && . bin/activate \
    pip3 install apache-flink==1.15.0 && \
    pip3 install pyflink

RUN python -m pip install apache-flink==1.15.0
# COPY apache-flink*.tar.gz /
# RUN pip3 install /apache-flink-libraries*.tar.gz && pip3 install /apache-flink*.tar.gz