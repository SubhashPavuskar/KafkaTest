FROM node:8.11.3-alpine

RUN apk update \
    && apk upgrade \
    && apk add curl

RUN mkdir -p /usr/src/app/postprocessor /usr/src/app/postprocessor/log

WORKDIR /usr/src/app/postprocessor

# Download and sets up confd for populating app-config file
ENV CONFD_SUB_VERSION=v0.11.0 CONFD_VERSION=confd-0.11.0-linux-amd64
ENV CONFD_URL=https://github.com/kelseyhightower/confd/releases/download/$CONFD_SUB_VERSION/$CONFD_VERSION
RUN mkdir -p /application/confd/bin \
 && curl -L -o /application/confd/bin/confd $CONFD_URL \
 && chmod +x /application/confd/bin/confd

# copy confd templates generated
COPY files/confd/  /application/confd

# copy app contents
COPY postprocessor /usr/src/app/postprocessor

# container CMD is the start script with confd
COPY files/start.sh /tmp/start.sh
RUN chmod +x /tmp/start.sh
COPY files/entrypoint.sh .
RUN chmod +x entrypoint.sh
#CMD [ "/tmp/start.sh" ]
ENTRYPOINT [ "./entrypoint.sh" ]
