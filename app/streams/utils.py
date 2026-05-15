import socket


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
