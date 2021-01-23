#!/usr/bin/env python3
from confluent_kafka import Consumer
import ccloud_lib as ccloud


if __name__ == "__main__":
    config = ccloud.read_ccloud_config("confluent.config")
    config["group.id"] = "activity"
    config["auto.offset.reset"] = "earliest"

    consumer = Consumer(config)
    consumer.subscribe(["inclass-2"])

    while True:
        msg = consumer.poll(1)

        if msg is None:
            print("wait")
            continue
        elif msg.error():
            print("error: {}".format(msg.error()))
        else:
            print("consume")

    consumer.close()
