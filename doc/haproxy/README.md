# Velo support use haproxy to load balance and readonly route

## haproxy config

```haproxy
global
    maxconn 100

defaults
    log global
    mode http
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

frontend only_master
    mode http
    bind *:5000
    default_backend only_master

backend only_master
    mode http
    option httpchk
    http-check send meth GET uri /?master
    http-check expect status 200
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 50 check
    server instance1 localhost:7380 maxconn 50 check

frontend both_master_or_slave
    mode http
    bind *:5100
    default_backend both_master_or_slave

backend both_master_or_slave
    mode http
    balance roundrobin
    option httpchk
    http-check send meth GET uri /?master_or_slave
    http-check expect status 200
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 20 check
    server instance1 localhost:7380 maxconn 20 check

frontend only_slave
    mode http
    bind *:5010
    default_backend only_slave

backend only_slave
    mode http
    balance roundrobin
    option httpchk
    http-check send meth GET uri /?slave
    http-check expect status 200
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 20 check
    server instance1 localhost:7380 maxconn 20 check

frontend only_slave_with_zone_zone1
    mode http
    bind *:5001
    default_backend only_slave_with_zone_zone1

backend only_slave_with_zone_zone1
    mode http
    balance roundrobin
    option httpchk
    http-check send meth GET uri /?lave_with_zone=zone1
    http-check expect status 200
    default-server inter 3s fall 3 rise 2 on-marked-down shutdown-sessions
    server instance0 localhost:7379 maxconn 20 check
    server instance1 localhost:7380 maxconn 20 check
```

## docker run
    
```bash
docker run -d --net host --name haproxy -v $PWD/haproxy.cfg:/usr/local/etc/haproxy/haproxy.cfg library/haproxy:2:2
```