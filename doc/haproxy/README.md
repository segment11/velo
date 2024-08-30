# velo support use haproxy to load balance and readonly route

## haproxy config

```haproxy
global
    maxconn 100

defaults
    log global
    mode tcp
    retries 2
    timeout client 30m
    timeout connect 4s
    timeout server 30m
    timeout check 5s

listen stats
    mode http
    bind *:7000
    stats enable
    stats uri /

listen only_master
    bind *:5000
    option httpchk
    http-check expect status 200 uri /?master
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 50 check
    server instance1 localhost:7380 maxconn 50 check

listen both_master_or_slave
    bind *:5100
    option httpchk
    http-check expect status 200 uri /?master_or_slave
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 20 check
    server instance1 localhost:7380 maxconn 20 check

 listen only_slave
     bind *:5010
     option httpchk
     http-check expect status 200 uri /?slave
     default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
     server instance0 localhost:7379 maxconn 20 check
     server instance1 localhost:7380 maxconn 20 check

 listen only_slave_with_zone_zone1
     bind *:5001
     option httpchk
     http-check expect status 200 uri /?slave_with_zone_zone1
     default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
     server instance0 localhost:7379 maxconn 20 check
     server instance1 localhost:7380 maxconn 20 check
```