FROM golang:1.3
MAINTAINER Colin Marc <colinmarc@gmail.com>

ADD . /go/src/github.com/colinmarc/sequins
RUN cd /go/src/github.com/colinmarc/sequins && make install

CMD ["--bind", ":9599", "/go/src/github.com/colinmarc/sequins/test_data"]
ENTRYPOINT ["/go/bin/sequins"]
EXPOSE 9599
