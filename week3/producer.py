#!/usr/bin/env python3
# import dependence
from confluent_kafka import Producer, KafkaError
import json
from datetime import date
import logging
import os
import sys
import time
import random
import ccloud_lib as ccloud

delivered_records = 0


def acked(err, msg):
    global delivered_records
    """Delivery report handler called on
    successful or failed delivery of message
    """
    if err is not None:
        logging.error("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        logging.info(
            "Produced record to topic {} partition [{}] @ offset {}".format(
                msg.topic(), msg.partition(), msg.offset()
            )
        )


def produce(config, topic, data, should_random):
    # construct producer
    producer = Producer(config)

    for one in data:
        # prepare message
        if should_random:
            record_key = random.randint(1, 5)
        else:
            record_key = "inclass-2"

        record_value = json.dumps(one)
        logging.info("Producing record: {}\t{}".format(record_key, record_value))
        producer.produce(
            topic,
            key=str(record_key),
            value=record_value,
            on_delivery=acked,
            partition=record_key,
        )
        time.sleep(0.25)
        # from previous produce() calls.
        producer.poll()

    producer.flush()

    # show record
    logging.info(
        "{} messages were produced to topic {}!".format(delivered_records, topic)
    )


def main(config_path, should_random=False):
    topic = "inclass-2"
    config = ccloud.read_ccloud_config(config_path)

    # set log file path
    log_path = "./log"
    os.makedirs(log_path, exist_ok=True)
    log_path = os.path.join(log_path, "{}_producer.log".format(date.today()))

    # set loggingfile
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            # logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )

    # load, and produce json data.
    data_list = "bcsample.json"
    logging.info(data_list)
    with open(data_list) as json_file:
        data = json.load(json_file)

    # normal running
    produce(config, topic, data, should_random)


if __name__ == "__main__":
    # normal mode
    main()
