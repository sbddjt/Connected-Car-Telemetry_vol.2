import json
import time

from confluent_kafka import Producer
from event_buffer import EventBuffer

class TelemetryProducer:
    def __init__(self, buffer: EventBuffer):
        self.buffer = buffer
        self.topic = "vehicle-telemetry-v1"

        self.enqueued_count = 0 # Producer 큐에 접수된 메세지
        self.delivered_count = 0 # 전송 성공
        self.failed_count = 0 # 최종 전송 실패

        self.producer = Producer(
            {
                "bootstrap.servers": ("localhost:29092, localhost:39092, localhost:49092"),
                "client.id": "sumo-telemetry-producer",
                "acks": "all",
                "enable.idempotence": True,
                "max.in.flight.requests.per.connection": 5,
                "message.timeout.ms": 30000,
            }
        )

    def _delivery_report(self, error, message):
        event = json.loads(message.value().decode("utf-8"))
        event_id = event["event_id"]

        if error is None:
            self.delivered_count += 1
            self.buffer.mark_delivered(event_id)
        else:
            self.failed_count += 1

            print(
                f"[Kafka 전송 실패] "
                f"event_id={event_id} | "
                f"error={error}"
            )

    def send(self, event: dict):
        message_key = event["vehicle_id"].encode("utf-8")

        message_value = json.dumps(
            event,
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")

        self.producer.produce(
            topic=self.topic,
            key=message_key,
            value=message_value,
            on_delivery=self._delivery_report,
        )

        self.enqueued_count += 1
        # 비동기 전송 결과 콜백 처리
        self.producer.poll(0)

    def drain_pending(self) -> None:
        while True:
            pending = self.buffer.get_pending(limit=1)

            if not pending:
                return

            event = pending[0]
            failed_before = self.failed_count

            self.send(event)

            # 현재 메세지의 최종 결과가 나올 때까지 기다립니다.
            # 기다리는 동안 성공, 실패 콜백도 실행됩니다.
            while self.producer.flush(timeout=1) > 0:
                pass

            if self.failed_count > failed_before:
                print(
                    f"[재전송 대기]"
                    f"event_id={event['event_id']} | 3초 후 재시도"
                )

                time.sleep(3)

    def close(self):
        remaining = self.producer.flush(timeout=10)

        print(
        f"[Kafka 전송 결과] "
        f"접수={self.enqueued_count} | "
        f"성공={self.delivered_count} | "
        f"실패={self.failed_count} | "
        f"미완료={remaining}"
        )