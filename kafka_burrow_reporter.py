from __future__ import print_function
import argparse
import httplib
import json
import re

# VERSION 0.1
# FROM https://github.com/jongho/kafka-burrow-telegraf-translator

# This code was written with inspiration from  kafka_jolokia_reporter.py (https://github.com/paksu/kafka-jolokia-telegraf-collector)


def get_http_response(conn, path):
    conn.request("GET", path)
    response = conn.getresponse()
    assert response.status == 200
    return response.read()


def get_clusters_from_burrow(conn):
    path="/v2/kafka"
    #print(path)

    response = json.loads(get_http_response(conn, path))
    if 'clusters' not in response:
        return []

    return response['clusters']


def get_consumers_from_burrow(conn, cluster):
    path="/v2/kafka/{}/consumer".format(cluster)
    #print(path)

    response = json.loads(get_http_response(conn, path))
    if 'consumers' not in response:
        return []

    return response['consumers']


def get_consumer_lag_status_from_burrow(conn, cluster, consumer):
    path="/v2/kafka/{}/consumer/{}/lag".format(cluster, consumer)
    #print(path)

    response = json.loads(get_http_response(conn, path))
    if 'status' not in response:
        return {}

    return response['status']


def fetch_consumer_lags_from_burrow(host, port):
    conn = httplib.HTTPConnection(host, port)
    consumer_lags = []

    for cluster in get_clusters_from_burrow(conn):
        for consumer in get_consumers_from_burrow(conn, cluster):
            consumer_lags.append(get_consumer_lag_status_from_burrow(conn, cluster, consumer))

    conn.close()
    return consumer_lags


def get_formated_str(dictionary, keys, prefix=''):
    return ','.join(['{}{}={}'.format(prefix, k, dictionary[k]) for k in keys])


def translate_lag_data(lag_data):
    """
    Parses a Kafka Consumer Lag data from burrow and converts it to set of InfluxDB Line protocol

    Currently supports at least Kafka 0.10.2 and Burrow (https://github.com/linkedin/Burrow 2017-03-07 commit)
    https://github.com/linkedin/Burrow
    https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_reference/

    EXAMPLE:
    - FROM:
        {
            "cluster": "test",
            "complete": true,
            "group": "TestGroup",
            "maxlag": null,
            "partition_count": 1,
            "partitions": [
                {
                    "end": {
                        "lag": 0,
                        "max_offset": 14132620,
                        "offset": 14132620,
                        "timestamp": 1491449760344
                    },
                    "partition": 0,
                    "start": {
                        "lag": 0,
                        "max_offset": 14132620,
                        "offset": 14132620,
                        "timestamp": 1491449751328
                    },
                    "status": "OK",
                    "topic": "Common-Test"
                },
                ...
            ],
            "status": "OK",
            "totallag": 0
        }

    - TO:
        kafka.consumer.lag,cluster=test,group=TestGroup status=OK,complete=true,totallag=0,partition_count=1
        kafka.consumer.tp.lag,cluster=test,group=TestGroup,topic=Common-Test,partition=0 status=OK,start.lag=0,start.max_offset=14132620,start.offset=14132620,start.timestamp=1491449751328,end.lag=0,end.max_offset=14132620,end.offset=14132620,end.timestamp=1491449751328
        ...
    """
    metrics = []

    # kafka.consumer.lag
    lag_measurement = 'kafka.consumer.lag'
    lag_tags = get_formated_str(lag_data, ['cluster', 'group'])
    lag_fields = get_formated_str(lag_data, ['status', 'complete', 'totallag', 'partition_count'])
    #print("lag_tags: {}".format(lag_tags))
    #print("lag_fields: {}".format(lag_fields))

    metrics.append("{},{} {}".format(lag_measurement, lag_tags, lag_fields))

    # kafka.consumer.tp.lag
    tp_lag_measurement = 'kafka.consumer.tp.lag'
    for tp_lag_data in lag_data['partitions']:
        #print("tp_lag_data: {}".format(tp_lag_data))
        tg_lag_tags = lag_tags + ',' + get_formated_str(tp_lag_data, ['topic', 'partition'])
        tg_lag_fields = get_formated_str(tp_lag_data, ['status']) + ',' + \
            get_formated_str(tp_lag_data['start'], ['lag', 'max_offset', 'offset', 'timestamp'], 'start.') + ',' + \
            get_formated_str(tp_lag_data['end'], ['lag', 'max_offset', 'offset', 'timestamp'], 'end.')
        metrics.append("{},{} {}".format(tp_lag_measurement, tg_lag_tags, tg_lag_fields))

    return metrics


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Kafka Burrow Reporter')
    parser.add_argument('--burrow-host', default='localhost', help='Burrow host')
    parser.add_argument('--burrow-port', type=int, default=8000, help='Burrow port')
    args = parser.parse_args()

    for lag_data in fetch_consumer_lags_from_burrow(args.burrow_host, args.burrow_port):
        for line in translate_lag_data(lag_data):
            print(line)
