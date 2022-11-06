from attrs import define
from datetime import datetime

from models.comms import Comms


def singleton(class_):
    instances = {}
    def getinstance(*args, **kwargs):
        if class_ not in instances:
            instances[class_] = class_(*args, **kwargs)
        return instances[class_]
    return getinstance


@singleton
@define
class Context:
    execution_date: datetime
    execution_date_utc: datetime
    temp_dir: str
    comms: Comms