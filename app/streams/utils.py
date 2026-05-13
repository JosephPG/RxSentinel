import socket
from asyncio import new_event_loop
from contextlib import contextmanager


@contextmanager
def loop_event():
    loop = new_event_loop()

    yield loop

    loop.run_forever()
    loop.close()


def kafka_consumer_conf(ops: dict = {}):
    return (
        {
            "group.id": "",
            "bootstrap.servers": "",
            "client.id": socket.gethostname(),
            "auto.offset.reset": "latest",  # El consumidor ignora el pasado. Solo leerá los mensajes nuevos que lleguen a partir del momento en que se conecta.
            "enable.auto.commit": "false",  # Apaga el guardado automático de progreso en Kafka.
            "enable.auto.offset.store": "false",  # Apaga el almacenamiento automático del progreso en la memoria local del cliente Kafka
            "partition.assignment.strategy": "cooperative-sticky",  # Define la estrategia para repartir las particiones entre los miembros del group.id
        }
        | ops
    )
