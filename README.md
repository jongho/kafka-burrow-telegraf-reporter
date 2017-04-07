# kafka-burrow-telegraf-reporter
>  Simple Kafka Consumer Lag metric reporter for telegraf with Burrow

Parses Kafka Consumer Lag metrics exposed via burrow and converts them to a set of InfluxDB Line protocol metrics.

**This code was written based on kafka_jolokia_reporter.py (https://github.com/paksu/kafka-jolokia-telegraf-collector)**

Currently supports at least Kafka 0.10.2 and Burrow (https://github.com/linkedin/Burrow 2017-03-07 commit)
- https://github.com/linkedin/Burrow
- https://docs.influxdata.com/influxdb/v1.2/write_protocols/line_protocol_reference/


## Requirements

Install and configure `burrow` to expose Kafka Consumer Lag metrics.

Setting `KAFKA_OPTS=-javaagent:/path/to/jolokia/jolokia-1.3.3/agents/jolokia-jvm.jar` when launching Kafka broker should be enough

## Usage

### How to run the script
```python kafka_burrow_reporter.py [--burrow-host] [--burrow-port]```

- `--burrow-host` defaults to `localhost`
- `--burrow-port` defaults to 8000

Example:
```python kafka_burrow_reporter.py --burrow-host=localhost --burrow-port=8778```

### Configure script to pass metrics to telegraf

The collector script works with Telegraf exec plugin.

Example configuration
```
[[inputs.exec]]
    commands = ["python /path/to/kafka_burrow_reporter.py"]
    data_format = "influx"
```
