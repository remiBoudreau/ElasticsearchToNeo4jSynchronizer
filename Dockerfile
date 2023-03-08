
# Latest stable 03/02/2023
ARG base_tag=3.17.2
FROM alpine:${base_tag}

# set up work directory space
WORKDIR /grayHatWarFareScraper
COPY ./* /grayHatWarFareScraper/.

# update and install deps
RUN \
    apk update && \
    apk upgrade && \
    # install Java'
    apk add --no-cache openjdk8-jre && \
    # install python package manager
    apk add --no-cache py-pip  && \
    # install python dependencies
    pip install -r requirements.txt

# java env vars
ENV JAVA_HOME=/opt/java/openjdk \
    PATH="/opt/java/openjdk/bin:$PATH"
    
# tika-pthon env vars; see https://github.com/chrismattmann/tika-python
ENV PYTHONIOENCODING utf8
ENV TIKA_VERSION default
ENV TIKA_SERVER_ENDPOINT: tika:9998
ENV TIKA_JAVA java
ENV TIKA_LOG_PATH /grayHatWarFareScraper/logs
ENV TIKA_LOG_FILE /grayHatWarFareScraper/logs/tika.log
# ENV TIKA_SERVER_JAR
# ENV TIKA_CLIENT_ONLY
# ENV TIKA_TRANSLATOR
# ENV TIKA_SERVER_CLASSPATH
# ENV TIKA_PATH
# ENV TIKA_STARTUP_SLEEP
# ENV TIKA_STARTUP_MAX_RETRY
# ENV TIKA_JAVA_ARGS 

# run script
ENTRYPOINT ["python","main.py"]