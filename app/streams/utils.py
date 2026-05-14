import os
import socket
from asyncio import new_event_loop, tasks
from contextlib import contextmanager

from loguru import logger


@contextmanager
def loop_event():
    try:
        loop = new_event_loop()
        yield loop

        loop.run_forever()
    except KeyboardInterrupt:
        """ See doc for asyncio Runner in close() method"""
        logger.info("Stop loop")

        to_cancel = tasks.all_tasks(loop)

        for task in to_cancel:
            task.cancel()

        if to_cancel:
            loop.run_until_complete(
                tasks.gather(*to_cancel, return_exceptions=True)
            )  # wait to cancel tasks
    finally:
        loop.stop()
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.run_until_complete(loop.shutdown_default_executor())
        loop.close()
        logger.info("Close loop")

        os._exit(0)


def kafka_consumer_conf(ops: dict = {}):
    """
    https://kafka.apache.org/42/configuration/consumer-configs/
    """
    return (
        {
            "group.id": "",
            "bootstrap.servers": "",
            "client.id": socket.gethostname(),
            "auto.offset.reset": "latest",
            "enable.auto.commit": "false",  # Apaga el guardado automático de progreso en Kafka.
            "enable.auto.offset.store": "false",  # Apaga el almacenamiento automático del progreso en la memoria local del cliente Kafka
            "partition.assignment.strategy": "cooperative-sticky",
        }
        | ops
    )
