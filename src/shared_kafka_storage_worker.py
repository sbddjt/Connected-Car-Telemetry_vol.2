"""Kafka 소비 묶음은 저장 완료 후 파티션별 오프셋을 함께 커밋합니다."""
import json
import time
from collections import deque
from datetime import datetime, timezone
from confluent_kafka import KafkaException, TopicPartition
from shared_fleet_telemetry_policy import Backoff
from shared_pipeline_metrics import PipelineMetrics


class KafkaStorageWorker:
    def __init__(self, consumer, store, metrics_name, retry_errors, batch_size=100,
                 retry_seconds=1, retry_max_seconds=30):
        self.consumer, self.store = consumer, store
        self.pending = deque()
        self.retry = Backoff(retry_seconds, retry_max_seconds)
        self.retry_errors = retry_errors + (KafkaException,)
        self.batch_size = batch_size
        self.paused = False
        self.total = 0
        self.delay = None
        self.metrics = PipelineMetrics(metrics_name)

    def on_revoke(self, consumer, partitions):
        revoked = {(p.topic, p.partition) for p in partitions}
        self.pending = deque(m for m in self.pending if (m.topic(),m.partition()) not in revoked)

    def _resume(self):
        assignment = self.consumer.assignment()
        if assignment:
            self.consumer.resume(assignment)
        self.paused = False
        self.retry.reset()

    def process_once(self):
        if self.pending and time.monotonic() >= self.retry.after:
            messages = list(self.pending)[:self.batch_size]
            events = [json.loads(message.value()) for message in messages]
            try:
                if self.batch_size == 1:
                    self.store.save(events[0])
                    self.consumer.commit(message=messages[0], asynchronous=False)
                else:
                    self.store.save_many(events)
                    # 모두 저장됐을 때만 각 파티션의 마지막 처리 위치 다음으로 이동합니다.
                    offsets = {}
                    for message in messages:
                        key = (message.topic(), message.partition())
                        offsets[key] = max(offsets.get(key,0), message.offset()+1)
                    self.consumer.commit(offsets=[TopicPartition(topic, partition, offset)
                                         for (topic,partition),offset in offsets.items()], asynchronous=False)
            except self.retry_errors as error:
                delay = self.retry.fail(time.monotonic())
                assignment = self.consumer.assignment()
                if assignment:
                    self.consumer.pause(assignment)
                self.paused = True
                print(f"[저장 재시도] {type(error).__name__} | {delay:.1f}초 후 | 미커밋 묶음 보관")
            else:
                for _ in messages:
                    self.pending.popleft()
                self.total += len(messages)
                newest = max(datetime.fromisoformat(event["event_time"].replace("Z", "+00:00")) for event in events)
                self.delay = max(0,(datetime.now(timezone.utc)-newest).total_seconds())
                if self.paused:
                    self._resume()
                self.retry.reset()
        if self.paused and not self.pending:
            self._resume()
        # 장애 때도 poll/consume으로 heartbeat와 재할당 이벤트를 처리합니다.
        if self.batch_size == 1:
            message = self.consumer.poll(.2)
            messages = [] if message is None else [message]
        else:
            messages = self.consumer.consume(self.batch_size, timeout=.1)
        for message in messages:
            if not message.error():
                self.pending.append(message)
        self.metrics.publish(self.total, processing_delay_seconds=self.delay,
                             retrying=self.paused, pending_messages=len(self.pending))
