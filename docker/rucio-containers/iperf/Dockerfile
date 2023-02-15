FROM alpine:latest

LABEL maintainer="Jensen Zhang"

RUN apk add --no-cache iperf openssh openrc \
        && mkdir -p /root/.ssh \
        && chmod 0700 /root/.ssh \
        && mkdir -p /run/openrc \
        && touch /run/openrc/softlevel

EXPOSE 22/tcp 5001/tcp 5001/udp

COPY docker-entrypoint.sh /docker-entrypoint.sh
RUN chmod +x /docker-entrypoint.sh

ENTRYPOINT ["/docker-entrypoint.sh"]

