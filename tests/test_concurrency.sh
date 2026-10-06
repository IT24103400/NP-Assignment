#!/bin/bash
# test_concurrency.sh - opens 5 simultaneous Controller connections to the Agent
# and shows they are all established at the same time. Uses bash's /dev/tcp,
# so it needs no extra tools. Run it while ./agent_400 is running.
PORT=9410
HOST=127.0.0.1
client() {
    local id=$1
    exec 3<>/dev/tcp/$HOST/$PORT || return 1
    printf 'AUTH OPS-3400\n' >&3
    sleep 6                                     # keep the connection open
    printf 'SYSINFO\nEXEC HOSTNAME\nQUIT\n' >&3
    timeout 3 cat <&3 > /tmp/ops_client_$id.out # read replies until the Agent closes
    exec 3>&-
}

for i in 1 2 3 4 5; do client $i & done
sleep 2

echo "=== Established connections to port $PORT (server side) ==="
ss -tn state established "( sport = :$PORT )"
echo "Count: $(ss -tn state established "( sport = :$PORT )" | tail -n +2 | wc -l)"

wait
echo
for i in 1 2 3 4 5; do
    echo "=== Client $i replies ==="
    cat /tmp/ops_client_$i.out
done
