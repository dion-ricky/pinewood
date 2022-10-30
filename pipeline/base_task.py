from abc import ABC
from typing import List, Union


class BaseTask(ABC):
    def __init__(self, task_id):
        self.task_id = task_id
        self.__prev = []
        self.__next = []

    def set_upstream(self, task: Union[List['BaseTask'], 'BaseTask']):
        if task in self.__prev:
            return

        if isinstance(task, BaseTask):
            self.__prev.append(task)
            task.set_downstream(self)
        elif isinstance(task, list):
            for t in task:
                self.set_upstream(t)
        else:
            raise Exception()

    @property
    def upstream(self) -> List['BaseTask']:
        """Direct upstream of this task"""
        return self.__prev

    def set_downstream(self, task: Union[List['BaseTask'], 'BaseTask']):
        if task in self.__next:
            return
        
        if isinstance(task, BaseTask):
            self.__next.append(task)
            task.set_upstream(self)
        elif isinstance(task, list):
            for t in task:
                self.set_downstream(t)
        else:
            raise Exception()

    @property
    def downstream(self) -> List['BaseTask']:
        """Direct downstream of this task"""
        return self.__next