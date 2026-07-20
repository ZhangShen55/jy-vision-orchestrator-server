import uuid


NAMESPACE = uuid.UUID("7e43c5dd-01ee-42e0-80f6-9029bbdfcbf5")


def stable_id(*parts: object) -> str:
    text = ":".join(str(part) for part in parts)
    return str(uuid.uuid5(NAMESPACE, text))
