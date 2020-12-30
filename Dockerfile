ARG BASE_CONTAINER=jupyter/pyspark-notebook
FROM $BASE_CONTAINER

LABEL maintainer="gnanpath project"
EXPOSE 5050
USER root

WORKDIR /opt/GnanaPath
COPY gnappsrv/ gnappsrv/
COPY gndatadis/ gndatadis/
COPY gnsampledata/ gnsampledata/
COPY gntests/ gntests/
COPY __init__.py __init__.py
COPY README.md README.md
COPY gndwdb/ gndwdb/
COPY gnsearch/ gnsearch/
COPY gnutils/  gnutils/
COPY Jenkinsfile Jenkinsfile
COPY startgnpath.sh startgnpath.sh
WORKDIR gnappsrv/

RUN pip install -r requirements.txt

#RUN useradd gnuser && chown -R gnuser /opt/GnanaPath

RUN mkdir /opt/GnanaPath/tmp
WORKDIR /opt/GnanaPath/gnappsrv

#RUN nohup python gnp_appsrv_main.py &
ENTRYPOINT ["/opt/conda/bin/tini", "--", "/opt/GnanaPath/startgnpath.sh"]
WORKDIR $HOME