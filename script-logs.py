import asyncio
import socket

from confluent_kafka.aio import AIOProducer

running = True


def print_send_log(err, msg):
    if err:
        print(f"Delivery error: {err}")
    else:
        print(
            f"Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}"
        )


async def run_producer(topic: str):
    conf = {
        "bootstrap.servers": "localhost:9092",
        "client.id": socket.gethostname(),
        "transactional.id": topic,
    }

    producer = AIOProducer(conf)

    await producer.init_transactions()

    transaction_active = False

    try:
        await producer.begin_transaction()

        transaction_active = True

        msgs = [
            await producer.produce(topic=topic, key=f"testkey{i}", value=f"testvalue{i}")
            for i in range(10)
        ]

        for msg in await asyncio.gather(*msgs):
            print(
                "Produced to: {} [{}] @ {}".format(
                    msg.topic(), msg.partition(), msg.offset()
                )
            )

        await producer.commit_transaction()

        transaction_active = False

    except Exception as _:
        raise
    finally:
        if transaction_active:
            await producer.abort_transaction()

        await producer.close()


if __name__ == "__main__":
    """
    create topic: ./kafka-topics.sh --bootstrap-server localhost:9092 --create --topic server-logs
    see messages: ./kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic server-logs --from-beginning
    """
    asyncio.run(run_producer("server-logs"))
