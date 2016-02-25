FROM golang:1.6

ADD . /go/src/github.com/stripe/sequins
RUN mkdir -p /build/
WORKDIR /go/src/github.com/stripe/sequins
CMD /go/src/github.com/stripe/sequins/jenkins_build.sh
