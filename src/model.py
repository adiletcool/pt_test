import json
from dataclasses import dataclass, field

from src.utils import get_current_timestamp


@dataclass
class RecordsModel:
    data: list[dict]
    timestamp: int = field(default_factory=get_current_timestamp)


@dataclass
class FilterQuery:
    keys: list[str]
    value: any  # str | int | float

    @classmethod
    def from_query(cls, query: str):
        keys, value = query.split('==')
        try:
            value = json.loads(value)
        except:
            pass
        return cls(keys=keys.split('.'), value=value)
