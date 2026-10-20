import json

from confluent_kafka import Consumer, KafkaException

from src.storage.postgres import connect, persist_event
from src.streaming.processor import process_event


def main() -> None:
    consumer = Consumer(
        {
            "bootstrap.servers": "localhost:9092",
            "group.id": "streamguard-consumer",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )

    topic = "streamguard.public.transactions"
    conn = None

    try:
        # Connect before consuming so database failures cannot lose events.
        conn = connect()
        consumer.subscribe([topic])

        print(f"Listening to Kafka topic: {topic}")
        print("Writing events to PostgreSQL before committing Kafka offsets.")
        print("Waiting for a message... Press Ctrl+C to stop.")

        while True:
            message = consumer.poll(1.0)

            if message is None:
                continue

            if message.error():
                raise KafkaException(message.error())

            raw_value = message.value()

            # Debezium can publish a null-valued Kafka tombstone.
            if raw_value is None:
                event = {"payload": None, "tombstone": True}
                result = None
            else:
                event = json.loads(raw_value.decode("utf-8"))
                result = process_event(event)

            raw_key = message.key()
            message_key = raw_key.decode("utf-8") if raw_key is not None else None

            # Persist first. This function commits its PostgreSQL transaction.
            # If it fails, execution stops before the Kafka offset is committed.
            is_new = persist_event(
                conn,
                event=event,
                topic=message.topic(),
                partition_id=message.partition(),
                kafka_offset=message.offset(),
                message_key=message_key,
                result=result,
            )

            if not is_new:
                print(
                    f"Duplicate event ignored: "
                    f"partition={message.partition()}, "
                    f"offset={message.offset()}"
                )
            elif result is None:
                print("Event has no after-row; recorded in the ledger.")
            else:
                transaction = result["transaction"]
                print(f"Operation: {result['operation']}")
                print(f"Transaction ID: {transaction['transaction_id']}")
                print(f"Amount: {transaction['amount']} {transaction['currency']}")

                if result["is_suspicious"]:
                    print("FRAUD ALERT")
                    print(f"Reason: {result['risk_reason']}")

            # Commit Kafka only after successful database persistence.
            consumer.commit(message=message, asynchronous=False)
            print("Offset committed successfully.")

    except KeyboardInterrupt:
        print("\nConsumer stopped.")
    finally:
        if conn is not None:
            conn.close()
        consumer.close()


if __name__ == "__main__":
    main()
