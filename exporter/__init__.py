#!/usr/bin/python
from prometheus_client import start_http_server, Summary, Gauge

import time

import subprocess

METRIC_PREFIX = 'kafka_consumer_group_'
gauges = {}


# Update Custom Gauge metric
def update_gauge(metric_name, label_dict, value):
    label_keys = tuple(label_dict.keys())
    label_values = tuple(label_dict.values())

    if metric_name not in gauges:
        gauges[metric_name] = Gauge(metric_name, '', label_keys)

    gauge = gauges[metric_name]

    if label_values:
        gauge.labels(*label_values).set(value)
    else:
        gauge.set(value)


# Execute OS command
def execute_os_command():
    cmd = "./kafka-consumer-groups.sh --new-consumer --describe --group dev --bootstrap-server localhost:9092 |awk 'NR>1 { print $2,$6 }'"
    p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                         stderr=subprocess.STDOUT)
    return iter(p.stdout.readline, '')


# Execute Update metrics
def execute_update():
    for line in execute_os_command():
        message = line.decode()

        message_lines = message.split(" ")
        if len(message_lines) < 2:
            break

        topic = message_lines[0]
        lag = message_lines[1]

        if lag != 'unknown\n':
            update_gauge(
                metric_name=METRIC_PREFIX + 'offset',
                label_dict={
                    'topic': topic,
                },
                value=int(lag)
            )


# Shutdown
def signal_handler(signum, frame):
    shutdown()


if __name__ == '__main__':
    # Start up the server to expose the metrics.
    start_http_server(8000)

    while True:
        execute_update()

        time.sleep(60)

