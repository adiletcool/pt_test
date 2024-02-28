from datetime import datetime


def get_current_timestamp() -> float:
    return datetime.now().timestamp()
