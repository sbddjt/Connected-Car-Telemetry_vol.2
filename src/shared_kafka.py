"""수신 서버와 설정 로더가 공유하는 Kafka 토픽 및 전송 오류."""
DEFAULT_KAFKA_TOPICS = {
    "V": "vehicle-telemetry-v1",
    "connectivity": "vehicle-connectivity-v1",
    "errors": "vehicle-errors-v1",
}


class DeliveryError(Exception):
    """Kafka의 최종 전송 성공을 확인하지 못했습니다."""
