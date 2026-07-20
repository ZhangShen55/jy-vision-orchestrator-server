#!/usr/bin/env python3
import argparse
import json
import time
import uuid
from pathlib import Path

from kafka import KafkaProducer


DEFAULT_BOOTSTRAP_SERVERS = "127.0.0.1:9092"
DEFAULT_TOPIC = "classroom_cv_task"
DEFAULT_MESSAGE_PATH = "tests/fixtures/lesson_message.json"


def load_payload(message_path: str) -> dict:
    with Path(message_path).open("r", encoding="utf-8") as file:
        payload = json.load(file)
    if not isinstance(payload, dict):
        raise ValueError("消息 JSON 必须是对象")
    return payload


def build_payload(args: argparse.Namespace) -> dict:
    payload = load_payload(args.message)
    if args.task_id:
        payload["task_id"] = args.task_id
    elif args.unique_task_id:
        base_task_id = str(payload.get("task_id") or "lesson-test")
        payload["task_id"] = f"{base_task_id}-{time.strftime('%Y%m%d-%H%M%S')}-{uuid.uuid4().hex[:6]}"
    if args.course_id:
        payload["course_id"] = args.course_id
    if args.student_count is not None:
        payload["student_count"] = args.student_count
    return payload


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="向 Kafka 模拟投递课堂视觉任务消息")
    parser.add_argument("--bootstrap-servers", default=DEFAULT_BOOTSTRAP_SERVERS, help="Kafka 地址")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="Kafka topic")
    parser.add_argument("--message", default=DEFAULT_MESSAGE_PATH, help="JSON 消息文件路径")
    parser.add_argument("--task-id", help="覆盖消息中的 task_id")
    parser.add_argument("--course-id", help="覆盖消息中的 course_id")
    parser.add_argument("--student-count", type=int, help="覆盖消息中的 student_count")
    parser.add_argument(
        "--no-unique-task-id",
        action="store_false",
        dest="unique_task_id",
        help="直接使用消息文件中的 task_id，不追加唯一后缀",
    )
    parser.set_defaults(unique_task_id=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_payload(args)
    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap_servers,
        acks="all",
        retries=3,
    )
    try:
        future = producer.send(
            args.topic,
            key=str(payload["task_id"]).encode("utf-8"),
            value=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        )
        record_metadata = future.get(timeout=10)
        producer.flush(timeout=10)
    finally:
        producer.close()

    print("Kafka 消息发送成功")
    print(f"  bootstrap_servers: {args.bootstrap_servers}")
    print(f"  topic: {record_metadata.topic}")
    print(f"  partition: {record_metadata.partition}")
    print(f"  offset: {record_metadata.offset}")
    print(f"  task_id: {payload['task_id']}")


if __name__ == "__main__":
    main()
