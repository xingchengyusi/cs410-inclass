#!/usr/bin/env python3
# import dependence
from confluent_kafka import Consumer, KafkaError, TopicPartition
import json
from datetime import date
import logging
import os
import sys
import ccloud_lib as ccloud


def main(config_path, handle_key, group_id="activity"):
    topic = "inclass-2"
    config = ccloud.read_ccloud_config(config_path)

    # set log file path
    log_path = "./log"
    os.makedirs(log_path, exist_ok=True)
    log_path = os.path.join(log_path, "{}_consumer.log".format(date.today()))

    # set loggingfile
    logging.basicConfig(
        level=logging.INFO,
        format="%(message)s",
        handlers=[
            # logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )

    consume(config=config, topic=topic, group_id=group_id, handle_key=handle_key)


def consume(
    config, topic, group_id="activity", auto_offset_reset="earliest", handle_key=0
):
    # complete consumer
    config["group.id"] = group_id
    config["auto.offset.reset"] = auto_offset_reset

    # construct consumer.
    consumer = Consumer(config)
    consumer.subscribe([topic])
    if handle_key != 0:
        consumer.assign([TopicPartition(topic, handle_key)])
    total_count = 0

    try:
        while True:
            msg = consumer.poll(1)

            if msg is None:
                logging.warning("Waiting for message or event/error in poll()")
                continue
            elif msg.error():
                logging.error("error: {}".format(msg.error()))
            else:
                total_count += 1
                logging.info(
                    "Consumed record with key {} and value {}, and updated total count to {}".format(
                        msg.key(), msg.value(), total_count
                    )
                )
    except KeyboardInterrupt:
        pass
    finally:
        logging.info("total consumed {} messages".format(total_count))
        consumer.close()


if __name__ == "__main__":
    main()
