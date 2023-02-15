#!/bin/sh

rc-status
rc-service sshd start
cp /tmp/authorized_keys /root/.ssh/authorized_keys
chmod 0700 /root/.ssh/authorized_keys
iperf -s
