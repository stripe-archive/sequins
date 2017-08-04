FROM golang:1.8.3

RUN apt-get update
RUN apt-get install -y build-essential autoconf libtool pkg-config unzip

RUN unzip -q /usr/stripe/deps/awscli-bundle.zip -d /tmp/ && \
  /tmp/awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

ADD . /go/src/github.com/stripe/sequins
RUN mkdir -p /build/
WORKDIR /go/src/github.com/stripe/sequins
CMD /go/src/github.com/stripe/sequins/jenkins_build.sh
